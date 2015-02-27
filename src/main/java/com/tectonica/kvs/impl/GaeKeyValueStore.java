/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-kvs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.kvs.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.tectonica.gae.GaeMemcacheLock;
import com.tectonica.kvs.AbstractIndex;
import com.tectonica.kvs.AbstractKeyValueStore;
import com.tectonica.kvs.Index;
import com.tectonica.kvs.Index.IndexMapper;
import com.tectonica.thirdparty.KryoUtil;
import com.tectonica.util.SerializeUtil;

public class GaeKeyValueStore<V extends Serializable> extends AbstractKeyValueStore<String, V>
{
	private final DatastoreService ds;

	private final Config<V> config;
	private final String kind;
	private final Key ancestor; // dummy parent for all entities to guarantee Datastore consistency
	private final List<GaeIndexImpl<?>> indexes;

	public static class Config<V>
	{
		private final Class<V> valueClass;
		private Serializer<V> serializer;
		private String namespace;
		private boolean usingNamespace = false;

		public static <V> Config<V> create(Class<V> valueClass)
		{
			return new Config<V>(valueClass);
		}

		private Config(Class<V> valueClass)
		{
			if (valueClass == null)
				throw new NullPointerException("valueClass");
			this.valueClass = valueClass;
		}

		public Config<V> withSerializer(Serializer<V> serializer)
		{
			this.serializer = serializer;
			return this;
		}

		public Config<V> withNamespace(String namespace)
		{
			this.namespace = namespace;
			this.usingNamespace = true;
			return this;
		}
	}

	public GaeKeyValueStore(Config<V> config, KeyMapper<String, V> keyMapper)
	{
		super(keyMapper);
		if (config == null)
			throw new NullPointerException("config");
		if (config.serializer == null)
			config.serializer = new JavaSerializer<V>();

		this.config = config;
		this.kind = config.valueClass.getSimpleName();
		this.indexes = new ArrayList<>();

		applyNamespace(); // needed before creation of key and initialization of cache
		this.ds = DatastoreServiceFactory.getDatastoreService();
		this.ancestor = KeyFactory.createKey(kind, BOGUS_ANCESTOR_KEY_NAME);

		// create cache now that all values (specifically 'kind') are initialized
		super.initializeCache();
	}

	@Override
	protected void initializeCache()
	{
		// prevent execution of the parent's attempt to create cache, we'll call it manually when we're ready
	}

	@Override
	protected Cache<String, V> createCache()
	{
		// we're returning a memcached-based wrapper here, so the 'kind' property needs to be set
		if (config.serializer instanceof JavaSerializer)
			return new JavaSerializeCache();
		return new CustomSerializeCache();
	}

	private Key keyOf(String key)
	{
		applyNamespace();
		return KeyFactory.createKey(ancestor, kind, key);
	}

	private void applyNamespace()
	{
		if (config.usingNamespace)
			NamespaceManager.set(config.namespace);
	}

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected V dbGet(String key)
	{
		try
		{
			return entityToValue(ds.get(keyOf(key)));
		}
		catch (EntityNotFoundException e)
		{
			return null;
		}
	}

	@Override
	public Iterator<KeyValue<String, V>> iterator()
	{
		return entryIteratorOfQuery(newQuery()); // query without filters = all
	}

	@Override
	public Iterator<String> keyIterator()
	{
		return keyIteratorOfQuery(newQuery().setKeysOnly());
	}

	@Override
	public Iterator<V> valueIterator()
	{
		return valueIteratorOfQuery(newQuery());
	}

	@Override
	protected Iterator<KeyValue<String, V>> dbOrderedIterator(Collection<String> keys)
	{
		if (keys.size() > 30)
			throw new RuntimeException("GAE doesn't support more than 30 at the time, need to break it");

		List<Key> gaeKeys = new ArrayList<>(keys.size());
		for (String key : keys)
			gaeKeys.add(keyOf(key));

		// we define a filter based on the IN operator, which returns values in the order of listing.
		// see: https://cloud.google.com/appengine/docs/java/datastore/queries#Java_Query_structure
		Filter filter = new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, FilterOperator.IN, gaeKeys);
		return entryIteratorOfQuery(newQuery().setFilter(filter));
	}

	@Override
	public boolean containsKey(String key)
	{
		Filter filter = new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, FilterOperator.EQUAL, keyOf(key));
		return ds.prepare(newQuery().setFilter(filter).setKeysOnly()).asIterator().hasNext();
	}

	/***********************************************************************************
	 * 
	 * SETTERS (UTILS)
	 * 
	 ***********************************************************************************/

	@Override
	protected Modifier<String, V> getModifier(final String key, ModificationType purpose)
	{
		// insert, replace and update are all treated the same in GAE: all simply do save()
		return new Modifier<String, V>()
		{
			@Override
			public V getModifiableValue()
			{
				// we use here same calls as if for read-only value, because in both cases a new instance is deserialized
				// NOTE: if we ever switch to a different implementation, with local objects, this wouldn't work
				return get(key, false);
			}

			@Override
			public void dbPut(V value)
			{
				save(key, value);
			}
		};
	}

	@Override
	public Lock getModificationLock(String key)
	{
		String prefix = config.usingNamespace ? config.namespace + "-" : "";
		String locksCacheNS = prefix + "lock-" + kind;
		return GaeMemcacheLock.getLock(key, true, locksCacheNS);
	}

	/***********************************************************************************
	 * 
	 * SETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected void dbInsert(String key, V value)
	{
		save(key, value);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	@Override
	protected boolean dbDelete(String key)
	{
		ds.delete(keyOf(key));
		return true; // we don't really know if the key previously existed
	}

	@Override
	protected int dbDeleteAll()
	{
		int removed = 0;
		for (Entity entity : ds.prepare(newQuery().setKeysOnly()).asIterable())
		{
			ds.delete(entity.getKey());
			removed++;
		}
		return removed; // an estimate. we have to assume that all keys existed before delete
	}

	/***********************************************************************************
	 * 
	 * INDEXES
	 * 
	 ***********************************************************************************/

	@Override
	public <F> Index<String, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc)
	{
		GaeIndexImpl<F> index = new GaeIndexImpl<>(mapFunc, indexName);
		indexes.add(index);
		return index;
	}

	/**
	 * GAE implementation of an index - simply exposes the Datastore property filters.
	 * 
	 * NOTE: Range queries require corresponding index configuration at 'datastore-indexes.xml'
	 * 
	 * @author Zach Melamed
	 */
	private class GaeIndexImpl<F> extends AbstractIndex<String, V, F>
	{
		public GaeIndexImpl(IndexMapper<V, F> mapFunc, String name)
		{
			super(mapFunc, name);
		}

		@Override
		public Iterator<KeyValue<String, V>> iteratorOf(F f)
		{
			return entryIteratorOfQuery(newIndexQuery(f));
		}

		@Override
		public Iterator<String> keyIteratorOf(F f)
		{
			return keyIteratorOfQuery(newIndexQuery(f).setKeysOnly());
		}

		@Override
		public Iterator<V> valueIteratorOf(F f)
		{
			return valueIteratorOfQuery(newIndexQuery(f));
		}

		@Override
		public Iterator<KeyValue<String, V>> iteratorOfRange(F from, F to)
		{
			return entryIteratorOfQuery(newIndexRangeQuery(from, to));
		}

		@Override
		public Iterator<String> keyIteratorOfRange(F from, F to)
		{
			return keyIteratorOfQuery(newIndexRangeQuery(from, to).setKeysOnly());
		}

		@Override
		public Iterator<V> valueIteratorOfRange(F from, F to)
		{
			return valueIteratorOfQuery(newIndexRangeQuery(from, to));
		}

		// ///////////////////////////////////////////////////////////////////////////////////////

		private Query newIndexQuery(F f)
		{
			Filter filter = new FilterPredicate(propertyName(), FilterOperator.EQUAL, f);
			return newQuery().setFilter(filter);
		}

		private Query newIndexRangeQuery(F from, F to)
		{
			if (from == null && to == null)
				throw new NullPointerException("both 'from' and 'to' are null");

			final String prop = propertyName();

			Filter fromFilter = null, toFilter = null;
			if (from != null)
				fromFilter = new FilterPredicate(prop, FilterOperator.GREATER_THAN_OR_EQUAL, from);
			if (to != null)
				toFilter = new FilterPredicate(prop, FilterOperator.LESS_THAN, to);

			final Filter filter;
			if (fromFilter != null && toFilter != null)
				filter = CompositeFilterOperator.and(fromFilter, toFilter);
			else
				filter = (fromFilter != null) ? fromFilter : toFilter;

			return newQuery().setFilter(filter);
		}

		private String propertyName()
		{
			return COL_NAME_INDEX_PREFIX + name;
		}

		private F getIndexedFieldOf(V value)
		{
			return mapper.getIndexedFieldOf(value);
		}
	}

	/***********************************************************************************
	 * 
	 * DATASTORE UTILS
	 * 
	 ***********************************************************************************/

	private static final String COL_NAME_ENTRY_VALUE = "value";
	private static final String COL_NAME_INDEX_PREFIX = "_i_";
	private static final String BOGUS_ANCESTOR_KEY_NAME = " ";

	private V entityToValue(Entity entity)
	{
		Blob blob = (Blob) entity.getProperty(COL_NAME_ENTRY_VALUE);
		return config.serializer.bytesToObj(blob.getBytes(), config.valueClass);
	}

	private Entity entryToEntity(String key, V value)
	{
		Entity entity = new Entity(kind, key, ancestor);
		entity.setUnindexedProperty(COL_NAME_ENTRY_VALUE, new Blob(config.serializer.objToBytes(value)));
		for (GaeIndexImpl<?> index : indexes)
		{
			Object field = (value == null) ? null : index.getIndexedFieldOf(value);
			entity.setProperty(index.propertyName(), field);
		}
		return entity;
	}

	private void save(String key, V value)
	{
		ds.put(entryToEntity(key, value));
	}

	private Query newQuery()
	{
		applyNamespace();
		return new Query(kind).setAncestor(ancestor);
	}

	private Iterator<KeyValue<String, V>> entryIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
		return new Iterator<KeyValue<String, V>>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public KeyValue<String, V> next()
			{
				final Entity entity = iter.next();
				return new KeyValue<String, V>()
				{
					@Override
					public String getKey()
					{
						return entity.getKey().getName();
					}

					@Override
					public V getValue()
					{
						return entityToValue(entity);
					}
				};
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	private Iterator<String> keyIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
		return new Iterator<String>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public String next()
			{
				return iter.next().getKey().getName();
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	private Iterator<V> valueIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
		return new Iterator<V>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public V next()
			{
				return entityToValue(iter.next());
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	/***********************************************************************************
	 * 
	 * SERIALIZATION
	 * 
	 ***********************************************************************************/

	public static interface Serializer<V>
	{
		V bytesToObj(byte[] bytes, Class<V> clz); // NOTE: if bytes is null, return null

		byte[] objToBytes(V obj); // NOTE: if obj is null, return null
	}

	private static final class JavaSerializer<V> implements Serializer<V>
	{
		@Override
		public V bytesToObj(byte[] bytes, Class<V> clz)
		{
			return SerializeUtil.bytesToObj(bytes, clz);
		}

		@Override
		public byte[] objToBytes(V obj)
		{
			return SerializeUtil.objToBytes(obj);
		}
	}

	public static class KryoSerializer<V> implements Serializer<V>
	{
		@Override
		public V bytesToObj(byte[] bytes, Class<V> clz)
		{
			return KryoUtil.bytesToObj(bytes, clz);
		}

		@Override
		public byte[] objToBytes(V obj)
		{
			return KryoUtil.objToBytes(obj);
		}
	}

	/*
	 * <dependency>
	 * <groupId>de.ruedigermoeller</groupId>
	 * <artifactId>fst</artifactId>
	 * <version>2.18</version>
	 * <scope>provided</scope>
	 * </dependency>
	 * 
	 * public static class FstSerializer<V> implements Serializer<V>
	 * {
	 * final FSTConfiguration conf;
	 * 
	 * public FstSerializer()
	 * {
	 * conf = FSTConfiguration.createDefaultConfiguration();
	 * conf.setShareReferences(false); // no circular references
	 * }
	 * 
	 * @SuppressWarnings("unchecked")
	 * 
	 * @Override
	 * public V bytesToObj(byte[] bytes, Class<V> clz)
	 * {
	 * return (bytes == null) ? null : (V) conf.asObject(bytes);
	 * }
	 * 
	 * @Override
	 * public byte[] objToBytes(V obj)
	 * {
	 * return (obj == null) ? null : conf.asByteArray(obj);
	 * }
	 * }
	 */

	/***********************************************************************************
	 * 
	 * CACHE IMPLEMENTATION
	 * 
	 ***********************************************************************************/

	private abstract class AbstractMemcachedBasedCache implements Cache<String, V>
	{
		protected final MemcacheService mc;

		protected AbstractMemcachedBasedCache()
		{
			String prefix = config.usingNamespace ? config.namespace + "-" : "";
			String kvsCacheNS = prefix + "kvs-" + kind;
			mc = MemcacheServiceFactory.getMemcacheService(kvsCacheNS);
		}
	}

	private class JavaSerializeCache extends AbstractMemcachedBasedCache
	{
		@Override
		@SuppressWarnings("unchecked")
		public V get(String key)
		{
			return (V) mc.get(key);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Map<String, V> get(Collection<String> keys)
		{
			return (Map<String, V>) (Map<String, ?>) mc.getAll(keys);
		}

		@Override
		public void put(String key, V value)
		{
			mc.put(key, value);
		}

		@Override
		public void put(Map<String, V> values)
		{
			mc.putAll(values);
		}

		@Override
		public void delete(String key)
		{
			mc.delete(key);
		}

		@Override
		public void deleteAll()
		{
			mc.clearAll();
		}
	};

	private class CustomSerializeCache extends AbstractMemcachedBasedCache
	{
		@Override
		public V get(String key)
		{
			byte[] bytes = (byte[]) mc.get(key);
			return config.serializer.bytesToObj(bytes, config.valueClass);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Map<String, V> get(Collection<String> keys)
		{
			Map<String, Object> values = mc.getAll(keys);
			Iterator<Entry<String, Object>> iter = values.entrySet().iterator();
			while (iter.hasNext())
			{
				Entry<String, Object> entry = iter.next();
				byte[] bytes = (byte[]) entry.getValue();
				entry.setValue(config.serializer.bytesToObj(bytes, config.valueClass));
			}
			return (Map<String, V>) (Map<String, ?>) values;
		}

		@Override
		public void put(String key, V value)
		{
			mc.put(key, config.serializer.objToBytes(value));
		}

		@Override
		@SuppressWarnings("unchecked")
		public void put(Map<String, V> values)
		{
			// NOTE: we make a huge assumption here, that we can modify the passed map. by doing so we rely on "inside information" that
			// this is harmless given how 'iteratorFor' is implemented
			Iterator<Entry<String, Object>> iter = ((Map<String, Object>) (Map<String, ?>) values).entrySet().iterator();
			while (iter.hasNext())
			{
				Entry<String, Object> entry = iter.next();
				Object value = entry.getValue();
				entry.setValue(config.serializer.objToBytes((V) value));
			}
			mc.putAll(values);
		}

		@Override
		public void delete(String key)
		{
			mc.delete(key);
		}

		@Override
		public void deleteAll()
		{
			mc.clearAll();
		}
	}
}
