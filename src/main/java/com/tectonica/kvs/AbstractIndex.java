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

package com.tectonica.kvs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tectonica.kvs.KeyValueStore.KeyValue;

public abstract class AbstractIndex<K, V, F> implements Index<K, V, F>
{
	protected final IndexMapper<V, F> mapper;
	protected final String name;

	protected AbstractIndex(IndexMapper<V, F> mapper, String name)
	{
		this.mapper = mapper;

		if (name == null || name.isEmpty())
			throw new RuntimeException("index name is mandatory in " + AbstractIndex.class.getSimpleName());
		this.name = name;
	}

	@Override
	public String getName()
	{
		return name;
	}

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public Set<K> keySetOf(F f)
	{
		return KvsUtil.iterateInto(keyIteratorOf(f), new HashSet<K>());
	}

	@Override
	public List<V> valuesOf(F f)
	{
		return KvsUtil.iterateInto(valueIteratorOf(f), new ArrayList<V>());
	}

	@Override
	public List<KeyValue<K, V>> entriesOf(F f)
	{
		return KvsUtil.iterateInto(iteratorOf(f), new ArrayList<KeyValue<K, V>>());
	}

	@Override
	public Iterable<KeyValue<K, V>> asIterableOf(F f)
	{
		return KvsUtil.iterableOf(iteratorOf(f));
	}

	@Override
	public Iterable<K> asKeyIterableOf(F f)
	{
		return KvsUtil.iterableOf(keyIteratorOf(f));
	}

	@Override
	public Iterable<V> asValueIterableOf(F f)
	{
		return KvsUtil.iterableOf(valueIteratorOf(f));
	}

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public Set<K> keySetOfRange(F from, F to)
	{
		return KvsUtil.iterateInto(keyIteratorOfRange(from, to), new HashSet<K>());
	}

	@Override
	public List<V> valuesOfRange(F from, F to)
	{
		return KvsUtil.iterateInto(valueIteratorOfRange(from, to), new ArrayList<V>());
	}

	@Override
	public List<KeyValue<K, V>> entriesOfRange(F from, F to)
	{
		return KvsUtil.iterateInto(iteratorOfRange(from, to), new ArrayList<KeyValue<K, V>>());
	}

	@Override
	public Iterable<KeyValue<K, V>> asIterableOfRange(F from, F to)
	{
		return KvsUtil.iterableOf(iteratorOfRange(from, to));
	}

	@Override
	public Iterable<K> asKeyIterableOfRange(F from, F to)
	{
		return KvsUtil.iterableOf(keyIteratorOfRange(from, to));
	}

	@Override
	public Iterable<V> asValueIterableOfRange(F from, F to)
	{
		return KvsUtil.iterableOf(valueIteratorOfRange(from, to));
	}

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean containsKeyOf(F f)
	{
		return (keyIteratorOf(f).hasNext());
	}

	@Override
	public KeyValue<K, V> getFirstEntry(F f)
	{
		return KvsUtil.firstOf(iteratorOf(f));
	}

	@Override
	public K getFirstKey(F f)
	{
		return KvsUtil.firstOf(keyIteratorOf(f));
	}

	@Override
	public V getFirstValue(F f)
	{
		return KvsUtil.firstOf(valueIteratorOf(f));
	}
}
