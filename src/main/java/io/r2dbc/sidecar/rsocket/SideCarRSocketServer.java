package io.r2dbc.sidecar.rsocket;

import io.netty.buffer.ByteBuf;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.netty.NettyPipeline;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import reactor.util.Loggers;

public class SideCarRSocketServer {

    public static void main(String[] args) throws InterruptedException {
        Hooks.onOperatorDebug();
        Loggers.useConsoleLoggers();
        TcpServer.create()
                 .host("localhost")
                 .port(8081)
                 .handle((inbound, outbound) ->
                     inbound.receive()
                            .map(ByteBuf::retain)
                            .as(outbound.options(NettyPipeline.SendOptions::flushOnEach)::send)
                 )
                 .bindNow();

        new SideCarRSocketServer().start()
                                  .subscribe();

        RSocket rSocket = RSocketFactory.connect()
                                      .transport(TcpClientTransport.create(8080))
                                      .start()
                                      .block();


        Thread.sleep(2000);

        rSocket.requestStream(DefaultPayload.create("Hello World"))
               .map(payload -> payload.getDataUtf8())
               .log()
               .blockLast();

    }

    Mono<Void> start() {
        return RSocketFactory.receive()
                .acceptor((setup, sendingSocket) -> {
                    return Mono.just(new AbstractRSocket() {
                        @Override
                        public Mono<Void> fireAndForget(Payload payload) {
                            return Mono.create(s ->
                                s.onDispose(
                                    TcpClient.create()
                                         .host("localhost")
                                         .port(8081)
                                         .connect()
                                         .flatMap(c ->
                                             c.outbound()
                                              .options(NettyPipeline.SendOptions::flushOnEach)
                                              .send(Mono.just(payload.sliceData().retain()))
                                              .then()
                                         )
                                         .doOnSuccess(s::success)
                                         .doOnError(s::error)
                                         .subscribe()
                                )
                            );
                        }

                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                            return Mono.create(s ->
                                    s.onDispose(
                                        TcpClient.create()
                                             .host("localhost")
                                             .port(8081)
                                             .connect()
                                             .flatMap(c ->
                                                     c.outbound()
                                                      .options(NettyPipeline.SendOptions::flushOnEach)
                                                      .send(Mono.just(payload.sliceData().retain()))
                                                      .then()
                                                      .thenMany(c.inbound().receive())
                                                      .reduce((l, r) ->
                                                          l.alloc().buffer(l.capacity(), r.capacity())
                                                           .writeBytes(l)
                                                           .writeBytes(r)
                                                      )
                                                      .map(DefaultPayload::create)
                                             )
                                             .doOnSuccess(s::success)
                                             .doOnError(s::error)
                                             .subscribe()
                                    )
                            );
                        }

                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                            return Flux.create(s ->
                                    s.onDispose(
                                        TcpClient.create()
                                             .host("localhost")
                                             .port(8081)
                                             .connect()
                                             .flatMapMany(c ->
                                                     c.outbound()
                                                      .options(NettyPipeline.SendOptions::flushOnEach)
                                                      .send(Mono.just(payload.sliceData().retain()))
                                                      .then()
                                                      .thenMany(c.inbound().receive())
                                                      .map(ByteBuf::retain)
                                                      .map(DefaultPayload::create)
                                             )
                                             .doOnNext(s::next)
                                             .doOnError(s::error)
                                             .subscribe()
                                    )
                            );
                        }

                        @Override
                        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                            return Flux.create(s ->
                                    s.onDispose(
                                            TcpClient.create()
                                                     .host("localhost")
                                                     .port(8081)
                                                     .connect()
                                                     .flatMapMany(c ->
                                                         Flux.merge(
                                                             Flux.from(payloads)
                                                                 .map(Payload::sliceData)
                                                                 .map(ByteBuf::retain)
                                                                 .as(c.outbound()
                                                                      .options(NettyPipeline.SendOptions::flushOnEach)::send)
                                                                 .then()
                                                                 .thenMany(Flux.empty()),
                                                             c.inbound()
                                                              .receive()
                                                              .map(DefaultPayload::create)
                                                         )
                                                     )
                                                     .doOnNext(s::next)
                                                     .doOnError(s::error)
                                                     .subscribe()
                                    )
                            );
                        }
                    });
                })
                .transport(TcpServerTransport.create(8080))
                .start()
                .flatMap(CloseableChannel::onClose);
    }
}
