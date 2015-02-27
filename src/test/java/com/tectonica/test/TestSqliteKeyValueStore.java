package com.tectonica.test;

import java.io.Serializable;

import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.KeyValueStore.KeyMapper;
import com.tectonica.kvs.impl.SqliteKeyValueStore;

public class TestSqliteKeyValueStore extends TestKeyValueStore
{
	@Override
	protected <V extends Serializable> KeyValueStore<String, V> createStore(Class<V> valueClz, KeyMapper<String, V> keyMapper)
	{
		return new SqliteKeyValueStore<>(valueClz, connStr(), keyMapper);
	}

	private String connStr()
	{
		String dbPath = TestKeyValueStore.class.getResource("/").getPath() + "test" + System.currentTimeMillis() + ".db";
		System.out.println(dbPath);
		return "jdbc:sqlite:" + dbPath;
	}
}
