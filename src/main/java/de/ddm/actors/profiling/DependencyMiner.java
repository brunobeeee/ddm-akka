package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<Set<String>> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		Integer result;
		int sourceFileIndex;
		int targetFileIndex;
		int sourceColumnIndex;
		int targetColumnIndex;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.sourceFileIndex = 0;
		this.targetFileIndex = 0;

		this.batches = new ArrayList<>();
		this.batchIds = new ArrayList<>();

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	
	private final File[] inputFiles;
	private final String[][] headerLines;

	private List<List<Set<String>>> batches;
	private List<Integer> batchIds;

	// Tracking and Logging
	private int filesRead = 0;
	private int resultsRecieved = 0;

	// Indexes to track wich file has to be compared to wich
	private int sourceFileIndex;
	private int targetFileIndex;

	// Indexes to track wich column has to be compared to wich
	private int sourceColumnIndex;
	private int targetColumnIndex;

	// Save the number of column combinations to determine stopping condition
	private int colCombinations;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
	
		if (message.getBatch().size() != 0) {


			// Check if batchId already exists
			int existingIndex = batchIds.indexOf(message.getId());

			if (existingIndex != -1) { // merge new batch with existing one
				List<Set<String>> existingBatch = this.batches.get(existingIndex);
				List<Set<String>> mergedBatch = mergeBatches(existingBatch, message.getBatch());
				this.batches.set(existingIndex, mergedBatch);
			} else { // add new batch at the end
				this.batches.add(message.getBatch());
				this.batchIds.add(message.getId());
			}

			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		} else {
			filesRead += 1;
			this.getContext().getLog().info(filesRead + " files read");

			if (filesRead >= inputFiles.length) {
				this.getContext().getLog().info("all files read");
				this.getContext().getLog().info("==============");

				// Calculate all combinations of cols for the stopping condition
				for (int i=0; i<this.headerLines.length; i++) {
					this.colCombinations += this.headerLines[i].length;
				}
				this.colCombinations = this.colCombinations * this.colCombinations -1;

				// give every worker a task
				for (ActorRef<DependencyWorker.Message> worker : this.dependencyWorkers) {
					incFileIndexes();
					if (sourceFileIndex >= batches.size()) // #task < #workers
						break;
					
					worker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,
																	batches.get(this.sourceFileIndex).get(this.sourceColumnIndex),
																	batches.get(this.targetFileIndex).get(this.targetColumnIndex),
																	batchIds.get(this.sourceFileIndex),
																	batchIds.get(this.targetFileIndex),
																	this.sourceColumnIndex,
																	this.targetColumnIndex));
				}

			}
		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
		}

		// Give worker the sourceFile, targetFile and their indexes for tracking
		this.sourceFileIndex = 0;
		this.targetFileIndex = 0;

		// Worker will recieve its first taskMessage in handle(batchMessage)
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		int result = message.getResult();
		int sourceBatchId = message.getSourceFileIndex();
		int targetBatchId = message.getTargetFileIndex();
		int sourceColumnIndex = message.getSourceColumnIndex();
		int targetColumnIndex = message.getTargetColumnIndex();

		if (this.headerLines[0] != null) {
			int dependent = sourceBatchId;
			int referenced = targetBatchId;
			File dependentFile = this.inputFiles[dependent];
			File referencedFile = this.inputFiles[referenced];
			List<InclusionDependency> inds = new ArrayList<>();

			if (result >= 0) { // -> a real result
				String[] dependentAttributes = {this.headerLines[dependent][sourceColumnIndex]};
				String[] referencedAttributes = {this.headerLines[referenced][targetColumnIndex]};

				// Write down inclusion dependency
				InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
				inds.add(ind);
			}

			if (inds.size() > 0)
				this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		this.resultsRecieved++;
		this.getContext().getLog().info("Recieved result " + this.resultsRecieved + "/" + colCombinations + " from sourceFile " + message.sourceFileIndex + " and targetFile " + message.targetFileIndex);


		try { // With more workers the function sometimes throws an indexOutOfBoundsError bc the critical section is not locked; we just catch the error in this case bc it means we are done with the calculation
			incFileIndexes();
		} catch (Exception e) {}

		// send new task if not done
		if (this.sourceFileIndex < batches.size()) {
			this.getContext().getLog().info("-------------------");
					dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy,
																	batches.get(this.sourceFileIndex).get(this.sourceColumnIndex),
																	batches.get(this.targetFileIndex).get(this.targetColumnIndex),
																	batchIds.get(this.sourceFileIndex),
																	batchIds.get(this.targetFileIndex),
																	this.sourceColumnIndex,
																	this.targetColumnIndex));
		}

		// stopping condition
		if (resultsRecieved >= this.colCombinations)
			this.end();
		return this;
	}

	// increment the file indexes such that every file gets compared with every other file
	private void incFileIndexes() {
		this.targetColumnIndex++;
		if (this.targetColumnIndex >= batches.get(this.targetFileIndex).size()) { // no more target files to read
			this.targetColumnIndex = 0;
			this.sourceColumnIndex++;
		}

		if (sourceColumnIndex >= batches.get(sourceFileIndex).size()) {
			this.sourceColumnIndex = 0;
			this.targetFileIndex++;
			if (this.targetFileIndex >= batches.size()) { // no more target files to read
				this.targetFileIndex = 0;
				this.sourceFileIndex++;
			}
		}
	}

	public static List<Set<String>> mergeBatches(List<Set<String>> batch1, List<Set<String>> batch2) {
        if (batch1.size() != batch2.size()) {
            throw new IllegalArgumentException("The lists do not have the same dimension.");
        }

        List<Set<String>> mergedList = new ArrayList<>();

        for (int i = 0; i < batch1.size(); i++) {
            Set<String> mergedSet = new HashSet<>(batch1.get(i));
            mergedSet.addAll(batch2.get(i));
            mergedList.add(mergedSet);
        }

        return mergedList;
    }

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);

		return this;
	}
}