package adiom;

import adiom.v1.Messages.ListDataResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;

public class CacheItem {
    private ListDataResponse lastItem;
    private long counter;
    private java.util.Iterator<ListDataResponse> it;

    public synchronized void next(long counter, StreamObserver<ListDataResponse> responseObserver) {
        if (this.counter == counter) {
            if (!it.hasNext()) {
                lastItem = ListDataResponse.newBuilder().build();
            } else {
                lastItem = it.next();
            }
            this.counter += 1;
            responseObserver.onNext(lastItem);
            responseObserver.onCompleted();
        } else if (this.counter > 0 && this.counter == counter + 1) {
            responseObserver.onNext(lastItem);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Invalid cursor " + counter + ", at " + this.counter)
                    .asException());
        }
    }

    public CacheItem(java.util.Iterator<ListDataResponse> it) {
        this.it = it;
        this.counter = 0;
    }
}
