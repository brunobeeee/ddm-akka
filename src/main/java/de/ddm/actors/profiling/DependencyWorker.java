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
import java.util.HashSet;

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

		List<List<Integer>> result = new ArrayList<>();

		this.getContext().getLog().info("Working on sourceFile " + sourceFileIndex + " and targetFile " + targetFileIndex);

		for (int i=0; i<sourceFile.size(); i++) {
			// Initialize a new list per sourceColumn that saves the indexes of the targetColumns it has inclusion dependencies to
			// e.g. if col 2 (sourceFile) has ind to col 4 (targetFile) then 4 gets added to the colIncIndexes on index 2 (sourceFile)
			List<Integer> colIncIndexes = new ArrayList<>();

			for (int j=0; j<targetFile.size(); j++) {
				if (sourceFileIndex == targetFileIndex && i == j) {
					// Do not compare columns with themselves
					continue;
				}

				List<String> sourceColumn = Arrays.asList(sourceFile.get(i));
				Set<String> targetColumn = new HashSet<>(Arrays.asList(targetFile.get(j))); // Directly convert to HashSet as we dont need the actual values anymore

				if (isSubset(sourceColumn, targetColumn)) {
					// Append the index of the targetColumn to signalize that sourceColumn has an IND to targetColumn
					colIncIndexes.add(j);
				}
			}
			result.add(colIncIndexes);
		}

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result, sourceFileIndex, targetFileIndex);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	public static boolean isSubset(List<String> list1, Set<String> list2) {
        for (String element : list1) {
            if (!list2.contains(element)) {
                return false;
            }
        }

        return true;
    }

}