import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.typed.javadsl.ActorFlow;
import akka.stream.typed.javadsl.ActorSink;

import java.math.BigDecimal;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class Main {


    public static void main(String[] args) {
       //source to generate 1 transaction every second
        Source<Integer, NotUsed> source = Source.repeat(1).throttle(1, Duration.ofSeconds(10));

        //flow to create a random transfer
        Flow<Integer, Transfer, NotUsed> generateTransfer = Flow.of(Integer.class).map(x -> {
            Random r = new Random();
            int accountFrom = r.nextInt(9) + 1;
            int accountTo;
            do {
                accountTo = r.nextInt(9) + 1;
            } while (accountTo == accountFrom);

            BigDecimal amount = new BigDecimal(r.nextInt(100000)).divide(new BigDecimal(100));
            Date date = new Date();

            Transaction from = new Transaction(accountFrom, BigDecimal.ZERO.subtract(amount), date);
            Transaction to = new Transaction(accountTo, amount, date);
            return new Transfer(from, to);
        });

        Flow<Transfer, Transaction, NotUsed> getTransactionsFromTransfer = Flow.of(Transfer.class)
                .mapConcat(transfer -> List.of(transfer.getFrom(), transfer.getTo()));

        Source<Integer, NotUsed> transactioIdSource = Source.fromIterator(() -> Stream.iterate(1, i -> i + 1).iterator());

        Sink<Transfer, CompletionStage<Done>> transferLogger = Sink.foreach(transfer -> {
            System.out.println("Transfer from " + transfer.getFrom().getAccountNumber() +
                    "to " + transfer.getTo().getAccountNumber() + " of " + transfer.getTo().getAmount());
        });






        Graph<SourceShape<Transaction>, NotUsed> sourcePartialGraph = GraphDSL.create(
                builder -> {
                    FanInShape2<Transaction, Integer, Transaction> assignTransactionIDs =
                            builder.add(ZipWith.create((trans, id) -> {
                                trans.setUniqueId(id);
                                return trans;
                            }));


                    builder.from(builder.add(transactioIdSource))
                            .via(builder.add(generateTransfer.alsoTo(transferLogger)))
                            .via(builder.add(getTransactionsFromTransfer))
                            .toInlet(assignTransactionIDs.in0());

                    builder.from(builder.add(transactioIdSource))
                            .toInlet(assignTransactionIDs.in1());

                    return SourceShape.of(assignTransactionIDs.out());
                }
        );


        ActorSystem<AccountManager.AccountManagerCommand> accountManager
                = ActorSystem.create(AccountManager.create(), "accountManager");

        Flow<Transaction, AccountManager.AddTransactionResponse, NotUsed> attemptToApplyTransaction =
                ActorFlow.ask(accountManager,Duration.ofSeconds(10), (trans, self) -> {
                    return new AccountManager.AddTransactionCommand(trans,self);
                });

        Sink<AccountManager.AddTransactionResponse, CompletionStage<Done>> rejectedTransactionSink = Sink.foreach(trans -> {
            System.out.println("**************************REJECTED transaction " + trans.getTransaction());
        });

        Flow<AccountManager.AddTransactionResponse, AccountManager.AccountManagerCommand, NotUsed>
                receiveResult = Flow.of(AccountManager.AddTransactionResponse.class).map(result ->{
            System.out.println("Logging " + result.getTransaction());
            return new AccountManager.DisplayBalanceCommand(result.getTransaction().getAccountNumber());
        });

        Sink<AccountManager.AccountManagerCommand, NotUsed> displayBalanceSink =
                ActorSink.actorRef(accountManager, new AccountManager.CompleteCommand(), (throwable) -> new AccountManager.FailedCommand());

        Source<Transaction, NotUsed> newSource = Source.fromGraph(sourcePartialGraph);

        newSource.via(attemptToApplyTransaction
                        .divertTo(rejectedTransactionSink, result -> !result.getSucceeded())
                )
                        .via(receiveResult)
                                .to(displayBalanceSink).run(accountManager);

    }
}