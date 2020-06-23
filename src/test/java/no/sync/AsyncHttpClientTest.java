package no.sync;

import com.ning.http.client.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.assertTrue;

/**
 * Simple test program to reproduce a threading issue in grizzly connection-pool, coming as a NullPointerException
 *
 * Forks 1000 requests to a non-listening serversocket, ignores the results.
 * Then replaces the server with a functioning webserver on the same port, and checks that requests work.
 *
 * Issue is intermittent, can often be caught by tools repeating test until failure.
 */
public class AsyncHttpClientTest {

    int selectedPort = 0;

    private static String PAYLOAD = "hello";

    private ExecutorService executorService;
    private AsyncHttpClient client;

    @Before
    public void setUp() {
        executorService = new ThreadPoolExecutor(20, 100, 2L, TimeUnit.SECONDS,
                new SynchronousQueue<>());

        AsyncHttpClientConfig clientConfig = new AsyncHttpClientConfig.Builder()
                .setReadTimeout(1000)
                .setRequestTimeout(1000)
                .setConnectTimeout(1000)
                .setExecutorService(executorService)
                .build();
        client = new AsyncHttpClient(clientConfig);
    }

    @After
    public void tearDown() {
        client.close();
        executorService.shutdown();
    }

    /**
     * Simple sebservice
     */
    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = PAYLOAD;
            t.sendResponseHeaders(200, response.length());

            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.flush();
            os.close();
            t.close();
        }
    }

    /**
     * Installs JDK webserver on the provided port serving 200 OK "hello"
     * @param port the port to bind to
     * @return the new HttpServer
     * @throws IOException if port bind fails
     */
    private static HttpServer installHttpServer(int port) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/foo", new MyHandler());
        httpServer.setExecutor(null); // creates a default executor
        httpServer.start();
        return httpServer;
    }

    /**
     * Replace given serversocket with a listening web server
     * @param serverSocket to shutdown
     * @throws IOException
     * @throws InterruptedException
     */
    private static void replaceServerSocketWithHttpServer(ServerSocket serverSocket)
            throws IOException, InterruptedException {
        int port = serverSocket.getLocalPort();
        serverSocket.close();
        installHttpServer(port);
    }

    /**
     * One-shot test to see that webserver functions
     */
    @Test
    public void testOnce() throws IOException, ExecutionException, InterruptedException {
        HttpServer httpServer = installHttpServer(0);
        int port = httpServer.getAddress().getPort();

        assertTrue(checkOneRequest(port));

        httpServer.stop(0);
    }

    /**
     * Fires a http request to the given port, blocks on response.
     * Ignores timeoutexception 2 times.
     */
    private boolean checkOneRequest(int port) throws ExecutionException, InterruptedException, IOException {
        int retries = 3;
        while(retries-- > 0) try {
            Response r = client.prepareGet("http://localhost:" + port + "/foo")
                    .execute()
                    .get();
            return 200 == r.getStatusCode() && PAYLOAD.equals(r.getResponseBody());
        }
        catch (ExecutionException uh) {
            if(! (uh.getCause() instanceof TimeoutException) ) {
                throw uh;
            }
        }
        return false;
    }

    /**
     * Wraps a Grizzly AsyncHandler into a CompletableFuture<Response>
     */
    private CompletableFuture<Response> grizzlyGet(String url) {
        final CompletableFuture<Response> response = new CompletableFuture<>();
        client.prepareGet("http://localhost:" + selectedPort + "/foo")
                .execute(new AsyncHandler<Response>() {
                    Response.ResponseBuilder responseBuilder = new Response.ResponseBuilder();
                    @Override
                    public void onThrowable(Throwable t) {
                        response.completeExceptionally(t);
                    }

                    @Override
                    public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                        responseBuilder.accumulate(bodyPart);
                        return STATE.CONTINUE;
                    }

                    @Override
                    public STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
                        responseBuilder.accumulate(responseStatus);
                        return STATE.CONTINUE;
                    }

                    @Override
                    public STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
                        responseBuilder.accumulate(headers);
                        return STATE.CONTINUE;
                    }

                    @Override
                    public Response onCompleted() throws Exception {
                        Response resp = responseBuilder.build();
                        response.complete(resp);
                        return resp;
                    }
                });
        return response;
    }

    /**
     * Main test
     * Fires 1000 requests,
     */
    @Test
    public synchronized void testLotsOfRequests() throws IOException, InterruptedException, ExecutionException {
        ServerSocket serverSocket = new ServerSocket(0);
        selectedPort = serverSocket.getLocalPort();
        System.out.println("Listening on " + serverSocket.getLocalSocketAddress());
        int iterations = 1000;
        AtomicBoolean allResult = new AtomicBoolean(true);
        ArrayList<CompletionStage<Response>> tests = new ArrayList<>();

        //Fork lots of failing requests towards non-listening serversocket
        for (int i = 0; i < iterations && allResult.get(); i++) {
            Thread.yield();
            CompletionStage<Response> stage =
                    grizzlyGet("http://localhost:" + selectedPort + "/foo")
                            .handle((result, throwable) -> {
                                if (throwable != null) {
                                    throw new RuntimeException(throwable);
                                }
                                return result;
                            })
                            .thenApply(response -> {
                                System.out.print('.');
                                try {
                                    if (response.getStatusCode() != 200 ||
                                            !PAYLOAD.equals(response.getResponseBody())) {
                                        allResult.set(false);
                                    }
                                } catch (IOException e) {
                                    allResult.set(false);
                                    e.printStackTrace();
                                }
                                return response;
                            })
                            .exceptionally(throwable -> {
                                Throwable cause = throwable.getCause().getCause();
                                if(cause instanceof IOException ||
                                        cause instanceof TimeoutException ||
                                        cause instanceof RejectedExecutionException) {
                                    System.out.print('.');
                                    return null;
                                }
                                cause.printStackTrace();
                                System.out.println();
                                System.out.print(cause.getMessage());
                                allResult.set(false);

                                return null;
                            });
            tests.add(stage);
        }

        System.out.println("Completed " + tests.size() + " requests. Installing proper server and try once");
        //Now install proper http server
        replaceServerSocketWithHttpServer(serverSocket);
        CompletableFuture.allOf(tests.stream()
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new)
        ).join();

        //And do one request, expecting success
        assertTrue(checkOneRequest(selectedPort));
    }


}
