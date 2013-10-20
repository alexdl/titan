package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

public class LRUVertexCache implements VertexCache {

    private final ConcurrentMap<Long, InternalVertex> volatileVertices;
    //private final ConcurrentMap<Long, InternalVertex> cache;

    public LRUVertexCache(final long capacity, final int concurrencyLevel) {
        volatileVertices = new MapMaker().concurrencyLevel(concurrencyLevel).makeMap();
        //cache = new ConcurrentLinkedHashMap.Builder<Long, InternalVertex>().maximumWeightedCapacity(capacity).concurrencyLevel(concurrencyLevel).build();
        /*
        cache = CacheBuilder.newBuilder().maximumSize(capacity).concurrencyLevel(concurrencyLevel)
                .removalListener(new RemovalListener<Long, InternalVertex>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, InternalVertex> notification) {
                        //Should only get evicted based on size constraint
                        //Preconditions.checkArgument(notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.EXPLICIT);

                        //if (notification.getKey() == null || notification.getValue() == null)
                        //    return;

                        //InternalVertex v = notification.getValue();
                        //if (v.hasAddedRelations()) {
                        //    volatileVertices.putIfAbsent(notification.getKey(), v);
                        //}
                    }
                })
                .build();*/
    }

    @Override
    public boolean contains(long id) {
        return volatileVertices.containsKey(id);
    }

    @Override
    public InternalVertex get(final long id, final Retriever<Long, InternalVertex> constructor) {
        Long vertexId = Long.valueOf(id);
        InternalVertex vertex = volatileVertices.get(vertexId);

        if (vertex == null) {
            InternalVertex newVertex = constructor.get(vertexId);
            vertex = volatileVertices.putIfAbsent(vertexId, newVertex);
            if (vertex == null)
                vertex = newVertex;
        }

        return vertex;
        /*try {
            return cache.get(id, new Callable<InternalVertex>() {
                @Override
                public InternalVertex call() throws Exception {
                    InternalVertex newVertex = volatileVertices.get(id);
                    return (newVertex == null) ? constructor.get(id) : newVertex;
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }*/
        /*
        InternalVertex v = addedRelVertices.get(id);

        if (v == null) {
            try {
                return cache.get(id, new Callable<InternalVertex>() {
                    @Override
                    public InternalVertex call() throws Exception {
                        return constructor.get(id);
                    }
                });
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return v;*/
    }

    @Override
    public void add(InternalVertex vertex, long vertexId) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(vertexId != 0);

        //Long id = Long.valueOf(vertexId);

        volatileVertices.put(vertexId, vertex);

        //if (vertex.isNew() || vertex.hasAddedRelations())
        //    volatileVertices.put(id, vertex);
    }

    @Override
    public List<InternalVertex> getAllNew() {
        ArrayList<InternalVertex> vertices = new ArrayList<InternalVertex>(10);
        for (InternalVertex v : volatileVertices.values()) {
            if (v.isNew())
                vertices.add(v);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        volatileVertices.clear();
        //cache.clear();
    }
}
