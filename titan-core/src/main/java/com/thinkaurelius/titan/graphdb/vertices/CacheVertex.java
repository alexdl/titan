package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.querycache.ConcurrentQueryCache;
import com.thinkaurelius.titan.graphdb.vertices.querycache.QueryCache;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {

    /*
     * queryCache must always be written after relationCache during lazy
     * initialization. Additionally, only queryCache may be used in
     * double-checked locking. I think these two constraints are sufficient to
     * make this class's code thread-safe, but breaking either constraint (or
     * both) would make it unsafe.
     * 
     * We could encapsulate both fields in a holder class. Using single holder
     * class reference would simplify the DCL and write ordering considerations.
     * But it would also make these instances have a fatter memory footprint in
     * their initialized state, which is by far the most common state.
     */
    private SortedSet<Entry> relationCache;
    private volatile QueryCache queryCache;

    public CacheVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);

        relationCache = new ConcurrentSkipListSet<Entry>();
        queryCache = new ConcurrentQueryCache();
    }

    @Override
    public Iterable<Entry> loadRelations(SliceQuery query, final Retriever<SliceQuery, List<Entry>> lookup) {
        if (isNew())
            return ImmutableList.of();

        if (hasLoadedRelations(query)) {
            return relationCache.subSet(StaticBufferEntry.of(query.getSliceStart(), null), StaticBufferEntry.of(query.getSliceEnd(), null));
        }

        List<Entry> results = lookup.get(query);
        relationCache.addAll(results);
        if (query.hasLimit() && results.size() < query.getLimit()) {
            query = query.updateLimit(Query.NO_LIMIT);
        }

        queryCache.add(query);
        return results;
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        return queryCache != null && queryCache.isCovered(query);
    }

}
