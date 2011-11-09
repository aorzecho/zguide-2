/**
 *  Majordomo Protocol broker
 *  A minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8
 *
 *  @author Arkadiusz Orzechowski <aorzecho@gmail.com>
 */
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class mdbroker {

    // We'd normally pull these from config data
    private static final int HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable
    private static final int HEARTBEAT_INTERVAL = 1000; // msecs
    private static final int HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL
            * HEARTBEAT_LIVENESS;

    // ---------------------------------------------------------------------

    /**
     * This defines a single service.
     */
    private static class Service {
        public final String name;// Service name
        Deque<ZMsg> requests = new ArrayDeque<ZMsg>();// List of client requests
        Deque<Worker> waiting = new ArrayDeque<Worker>();// List of waiting
                                                         // workers

        public Service(String name) {
            this.name = name;
        }
    }

    /**
     * This defines one worker, idle or active.
     */
    private static class Worker {
        String identity;// Identity of worker
        ZFrame address;// Address frame to route to
        Service service; // Owning service, if known
        long expiry;// Expires at unless heartbeat

        public Worker(ZFrame address) {
            this.address = address;
            this.expiry = System.currentTimeMillis() + HEARTBEAT_INTERVAL
                    * HEARTBEAT_LIVENESS;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(address.getData());
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Worker))
                return false;
            Worker other = (Worker) obj;
            return Arrays.equals(address.getData(), other.address.getData());
        }

    }

    // ---------------------------------------------------------------------

    private ZContext ctx;
    private String endpoint; // Broker binds to this endpoint

    private ZMQ.Socket socket; // Socket for clients & workers
    private long heartbeatAt;// When to send HEARTBEAT
    private boolean verbose = false;// Print activity to stdout
    private Formatter fmt = new Formatter(System.out);

    public mdbroker(boolean verbose) {
        this.verbose = verbose;
    }

    private int liveness;// How many attempts left
    private int heartbeat = 2500;// Heartbeat delay, msecs
    private int reconnect = 2500; // Reconnect delay, msecs

    /**
     * Hash of known services
     */
    private Map<String, Service> services = new HashMap<String, Service>();
    /**
     * 
     */
    private WorkersPool workers = new WorkersPool();// manages all known workers

    private static class WorkersPool {
        private Deque<Worker> workers = new ArrayDeque<Worker>();
        private static final ZFrame heartbeatFrame = new ZFrame(PPP_HEARTBEAT);
        private long heartbeatAt = System.currentTimeMillis()
                + HEARTBEAT_INTERVAL;

        /**
         * Worker is ready, remove if on list and move to end
         */
        public synchronized void workerReady(Worker worker) {
            if (workers.remove(worker)) {
                System.out.printf("I:    %s is alive, waiting\n",
                        worker.address.toString());
            } else {
                System.out.printf("I: %s is now ready to work\n",
                        worker.address.toString());
            }
            workers.offerLast(worker);
        }

        /**
         * Return next available worker address
         */
        public synchronized ZFrame next() {
            return workers.pollFirst().address;
        }

        /**
         * Send heartbeats to idle workers if it's time
         */
        public synchronized void sendHeartbeats(Socket backend) {
            // Send heartbeats to idle workers if it's time
            if (System.currentTimeMillis() >= heartbeatAt) {
                for (Worker worker : workers) {
                    worker.address.sendAndKeep(backend, ZMQ.SNDMORE);
                    heartbeatFrame.sendAndKeep(backend);
                }
                heartbeatAt = System.currentTimeMillis() + HEARTBEAT_INTERVAL;
            }
        }

        /**
         * Look for & kill expired workers. Workers are oldest to most recent,
         * so we stop at the first alive worker.
         */
        public synchronized void purge() {
            for (Worker w = workers.peekFirst(); w != null
                    && w.expiry < System.currentTimeMillis(); w = workers
                    .peekFirst()) {
                workers.pollFirst().address.destroy();
            }
        }

        public boolean isEmpty() {
            return workers.isEmpty();
        }

        public synchronized void close() {
            for (Worker worker : workers)
                worker.address.destroy();
        }

        public Worker findOrCreate(ZFrame sender) {
            // TODO Auto-generated method stub
            return null;
        }

        public Worker get(ZFrame sender) {
            // TODO Auto-generated method stub
            return null;
        }

        public void delete(ZFrame sender) {
            // TODO Auto-generated method stub

        }

        public void workerReady(ZFrame sender, ZFrame service) {
            // TODO Auto-generated method stub

        }
    }

    /**
     * Main broker work happens here
     */
    public void mediate(String[] args) {
        boolean verbose = (args.length > 0 && "-v".equals(args[0]));
        mdbroker broker = new mdbroker(verbose);
        broker.bind("tcp://*:5555"); // Can be called multiple times with
                                     // different endpoints

        while (!Thread.currentThread().isInterrupted()) {
            ZMQ.Poller items = ctx.getContext().poller();
            items.register(socket, ZMQ.Poller.POLLIN);
            if (items.poll(HEARTBEAT_INTERVAL * 1000) == -1)
                break; // Interrupted
            if (items.pollin(0)) {
                // receive whole message (all ZFrames) at once
                ZMsg msg = ZMsg.recvMsg(socket);
                if (msg == null)
                    break; // Interrupted

                if (verbose)
                    fmt.format("I: received message: \n%s", msg.toString());

                ZFrame sender = msg.pop();
                ZFrame empty = msg.pop();
                ZFrame header = msg.pop();

                if (MDP.C_CLIENT.frameEquals(header))
                    processClient(sender, msg);
                else if (MDP.W_WORKER.frameEquals(header))
                    processWorker(sender, msg);
                else {
                    fmt.format("E: invalid message:\n%s", msg.toString());
                    msg.destroy();
                }

                sender.destroy();
                empty.destroy();
                header.destroy();

            }
            workers.sendHeartbeats(socket);
            workers.purge();

        }

        // When we're done, clean up properly
        workers.close();
        context.destroy();
    }

    private void processWorker(ZFrame sender, ZMsg msg) {
        assert (msg.size() >= 1); // At least, command

        ZFrame command = msg.pop();

        Worker worker = workers.get(sender);

        if (MDP.W_READY.frameEquals(command)) {
            // Not first command in session || Reserved service name
            if (worker != null || sender.toString().startsWith("mmi."))
                workers.delete(sender);
            else {
                // Attach worker to service and mark as idle
                ZFrame serviceFrame = msg.pop();
                workers.workerReady(sender, serviceFrame);
                serviceFrame.destroy();
            }
        } else if (MDP.W_REPLY.frameEquals(command)) {
            if (worker != null) {
                // Remove & save client return envelope and insert the
                // protocol header and service name, then rewrap envelope.
                ZFrame client = msg.unwrap();
                msg.addFirst(new ZFrame(worker.service.name));
                msg.addFirst(MDP.C_CLIENT.newFrame());
                msg.wrap(client);
                msg.send(socket);
                workers.workerReady(worker);
            } // TODO else should we disconnect?
        } else if (MDP.W_HEARTBEAT.frameEquals(command)) {
            if (worker != null)
                worker.expiry = System.currentTimeMillis() + HEARTBEAT_EXPIRY;
        } else if (MDP.W_DISCONNECT.frameEquals(command))
            workers.delete(sender);
        else
            fmt.format("E: invalid message:\n%s", msg.toString());
        msg.destroy();
    }

    private void processClient(ZFrame sender, ZMsg msg) {
        // TODO Auto-generated method stub

    }

    private void bind(String string) {
        // TODO Auto-generated method stub

    }
}
