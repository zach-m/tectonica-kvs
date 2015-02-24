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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tectonica.kvs.KeyValueStore.KeyValue;

public interface Index<K, V, F>
{
	public static interface IndexMapper<V, F>
	{
		public F getIndexedFieldOf(V value);
	}

	String getName();

	Iterator<KeyValue<K, V>> iteratorOf(F f);

	Iterator<K> keyIteratorOf(F f);

	Iterator<V> valueIteratorOf(F f);

	boolean containsKeyOf(F f);

	Set<K> keySetOf(F f);

	List<V> valuesOf(F f);

	List<KeyValue<K, V>> entriesOf(F f);

	Iterable<KeyValue<K, V>> asIterableOf(F f);

	Iterable<K> asKeyIterableOf(F f);

	Iterable<V> asValueIterableOf(F f);

	KeyValue<K, V> getFirstEntry(F f);

	K getFirstKey(F f);

	V getFirstValue(F f);
}