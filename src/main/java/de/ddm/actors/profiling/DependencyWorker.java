package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		List<String[]> sourceFile;
		List<String[]> targetFile;
		int sourceFileIndex;
		int targetFileIndex;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		List<String[]> sourceFile = message.getSourceFile();
		List<String[]> targetFile = message.getTargetFile();
		int sourceFileIndex = message.getSourceFileIndex();
		int targetFileIndex = message.getTargetFileIndex();

		this.getContext().getLog().info("Recieved sourceFile " + sourceFileIndex + " and targetFile " + targetFileIndex);
		
		List<List<Integer>> result = new ArrayList<>();

		// Reset the list as it gets reused over multiple messages
		result = new ArrayList<>();

		for (int i=0; i<sourceFile.get(0).length; i++) {
			// Initialize a new list per sourceColumn that saves the indexes of the targetColumns it has inclusion dependencies to
			// e.g. if col 2 (sourceFile) has a ind to col 4 (targetFile) then 4 gets added to the colIncIndexes on index 2 (sourceFile)
			List<Integer> colIncIndexes = new ArrayList<>();

			for (int j=0; j<targetFile.get(0).length; j++) {
				if (i == j) {
					// Do not compare columns with themselves
					continue;
				}

				List<String> sourceColumn = getColumn(sourceFile, i);
				List<String> targetColumn = getColumn(targetFile, j);

				// True until an Element in sourceColumn cannot be found in targetColumn
				boolean inclusionDependency = true;

				for (String sourceElement : sourceColumn) {
					// False until sourceElement gets found in targetColumn
					boolean foundMatch = false;
					for (String targetElement : targetColumn) {
						if (sourceElement.equals(targetElement)) {
							foundMatch = true;
							break;
						}
					}

					// If no match can be found there is no inclusion Dependency
					if (!foundMatch) {
						inclusionDependency = false;
						break;
					}
				}
				if (inclusionDependency) {
					// Append the index of the targetColumn to signalize that sourceColumn has an inc to targetColumn
					colIncIndexes.add(j);
				}
			}
			result.add(colIncIndexes);
		}

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result, sourceFileIndex, targetFileIndex);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private List<String> getColumn(List<String[]> table, int index) {
		List<String> result = new ArrayList<>();

		for (int r = 0; r < table.size(); r++) {
			result.add(table.get(r)[index]);
		}

		return result;
	}

}
