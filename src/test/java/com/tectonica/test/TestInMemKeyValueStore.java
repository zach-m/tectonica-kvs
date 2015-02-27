package com.tectonica.test;

import java.io.Serializable;

import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.KeyValueStore.KeyMapper;
import com.tectonica.kvs.impl.InMemKeyValueStore;

public class TestInMemKeyValueStore extends TestKeyValueStore
{
	@Override
	protected <V extends Serializable> KeyValueStore<String, V> createStore(Class<V> valueClz, KeyMapper<String, V> keyMapper)
	{
		return new InMemKeyValueStore<>(keyMapper);
	}
}
