import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.*;
import akka.stream.javadsl.*;

import java.math.BigDecimal;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class Main {


    public static void main(String[] args) {

        Map<Integer, Account> accounts = new HashMap<>();

        //set up accounts
        for (int i = 1; i <= 10; i++) {
            accounts.put(i, new Account(i, new BigDecimal(1000)));
        }

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

        Flow<Transaction, Transaction, NotUsed> applyTransactionsToAccount = Flow.of(Transaction.class).map(trans -> {
            Account account = accounts.get(trans.getAccountNumber());
            account.addTransaction(trans);
            System.out.println("Account " + account.getId() + " now has balance of " + account.getBalance());

            return trans;
        });

        Sink<Transaction, CompletionStage<Done>> rejectedTransactionSink = Sink.foreach(trans -> {
            System.out.println("REJECTED transaction " + trans + " as account balance is "
                    + accounts.get(trans.getAccountNumber()).getBalance());
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

        Graph<SinkShape<Transaction>, CompletionStage<Done>> sinkPartialGraph = GraphDSL.create(
                Sink.foreach(System.out::println), (builder, out) -> {
                    FlowShape<Transaction, Transaction> entryFlow =
                            builder.add(Flow.of(Transaction.class)
                                    .divertTo(rejectedTransactionSink, trans -> {
                                        Account account = accounts.get(trans.getAccountNumber());
                                        BigDecimal forecastBalance = account.getBalance().add(trans.getAmount());
                                        return (forecastBalance.compareTo(BigDecimal.ZERO) < 0);
                                    }));

                    builder.from(entryFlow)
                            .via(builder.add(applyTransactionsToAccount))
                            .to(out);
                    return SinkShape.of(entryFlow.in());
                }
        );
        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
                GraphDSL.create(sinkPartialGraph, (builder, out) -> {
                    builder.from(builder.add(sourcePartialGraph))
                            .to(out);
                    return ClosedShape.getInstance();
                })
        );
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");
        graph.run(actorSystem);
    }
}