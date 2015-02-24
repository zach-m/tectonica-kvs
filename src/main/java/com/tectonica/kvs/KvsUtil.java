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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tectonica.kvs.KeyValueStore.KeyValue;

public class KvsUtil
{
	private static class RawKeyValue<K, V> implements KeyValue<K, V>
	{
		private final K key;
		private final V value;

		public RawKeyValue(K key, V value)
		{
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey()
		{
			return key;
		}

		@Override
		public V getValue()
		{
			return value;
		}
	}

	public static <K, V> KeyValue<K, V> keyValueOf(final K key, final V value)
	{
		return new RawKeyValue<K, V>(key, value);
	}

	public static <T, C extends Collection<T>> C iterateInto(Iterator<T> iter, C collection)
	{
		while (iter.hasNext())
			collection.add(iter.next());
		return collection;
	}

	public static <T> Iterable<T> iterableOf(final Iterator<T> iter)
	{
		return new Iterable<T>()
		{
			@Override
			public Iterator<T> iterator()
			{
				return iter;
			}
		};
	}

	public static <T> T firstOf(final Iterator<T> iter)
	{
		if (iter.hasNext())
			return iter.next();
		return null;
	}

	public static <K, V> List<KeyValue<K, V>> orderByKeys(Map<K, V> entries, Collection<K> keys)
	{
		List<KeyValue<K, V>> ordered = new ArrayList<>();
		for (K key : keys)
		{
			V value = entries.get(key);
			if (value != null)
				ordered.add(new RawKeyValue<K, V>(key, value));
		}
		return ordered;
	}
}
