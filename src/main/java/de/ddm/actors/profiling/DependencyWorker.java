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
		Set<String> sourceFile;
		Set<String> targetFile;
		int sourceFileIndex;
		int targetFileIndex;
		int sourceColumnIndex;
		int targetColumnIndex;
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
		Set<String> sourceFile = message.getSourceFile();
		Set<String> targetFile = message.getTargetFile();
		int sourceFileIndex = message.getSourceFileIndex();
		int sourceColumnIndex = message.getSourceColumnIndex();
		int targetFileIndex = message.getTargetFileIndex();
		int targetColumnIndex = message.getTargetColumnIndex();

		Integer result = -1;

		this.getContext().getLog().info("Working on sourceFile " + sourceFileIndex + " and targetFile " + targetFileIndex);
		this.getContext().getLog().info("col " + sourceColumnIndex + " | col " + targetColumnIndex);

		if (sourceFileIndex != targetFileIndex || sourceColumnIndex != targetColumnIndex) {
			if (isSubset(sourceFile, targetFile)) {
				// Append the index of the targetColumn to signalize that sourceColumn has an IND to targetColumn
				result = targetColumnIndex;
			}
		}

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(),result, sourceFileIndex, targetFileIndex, sourceColumnIndex, targetColumnIndex);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	public static boolean isSubset(Set<String> list1, Set<String> list2) {
        for (String element : list1) {
            if (!list2.contains(element)) {
                return false;
            }
        }

        return true;
    }

}