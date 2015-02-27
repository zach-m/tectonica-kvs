package com.tectonica.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tectonica.kvs.Index;
import com.tectonica.kvs.Index.IndexMapper;
import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.KeyValueStore.KeyMapper;
import com.tectonica.kvs.Updater;
import com.tectonica.test.model.Stopover;
import com.tectonica.test.model.Topic;
import com.tectonica.test.model.Topic.TopicKind;

public abstract class TestKeyValueStore
{
	protected abstract <V extends Serializable> KeyValueStore<String, V> createStore(Class<V> valueClz, KeyMapper<String, V> keyMapper);

	// //////////////////////////////////////////////////////////////////////////////////////////

	@Before
	public void setUp()
	{}

	@After
	public void tearDown()
	{}

	// //////////////////////////////////////////////////////////////////////////////////////////

	@Test
	public void testKVS()
	{
		KeyValueStore<String, Topic> store;
		Index<String, Topic, String> index;

		store = createStore(Topic.class, new KeyMapper<String, Topic>()
		{
			@Override
			public String getKeyOf(Topic topic)
			{
				return topic.topicId;
			}
		});

		index = store.createIndex("b2t", new IndexMapper<Topic, String>()
		{
			@Override
			public String getIndexedFieldOf(Topic topic)
			{
				return topic.bundle();
			}
		});

		Topic t1, t2, t3, t4, t;
		List<Topic> l;
		store.deleteAll();
		store.addValue(t1 = new Topic("001", "type1", TopicKind.AAA));
		store.addValue(t2 = new Topic("002", "type1", TopicKind.AAA));
		store.addValue(t3 = new Topic("003", "type3", TopicKind.AAA));
		store.addValue(t4 = new Topic("004", "type3", TopicKind.AAA));
		store.clearCache();

		assertTrue(store.containsKey("001"));
		assertFalse(store.containsKey("xxx"));

		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (t = store.get("001")));
		assertEquals(t, t1);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("001", "002"))));
		assertEquals(l, Arrays.asList(t1, t2));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("002", "002"))));
		assertEquals(l, Arrays.asList(t2, t2));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("xxx", "yyy"))));
		assertTrue(l.size() == 0);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("aaa", "003", "yyy", "002", "xxx", "001", "004"))));
		assertEquals(l, Arrays.asList(t3, t2, t1, t4));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only type3:");
		System.err.println("[TEST]  " + (l = index.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.equals(Arrays.asList(t3, t4)) || l.equals(Arrays.asList(t4, t3)));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only types 0..1:");
		System.err.println("[TEST]  "
				+ (l = index.valuesOfRange(Topic.bundle("type0", TopicKind.AAA), Topic.bundle("type1", TopicKind.AAA))));
		assertTrue(l.isEmpty());
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only types 1..3:");
		System.err.println("[TEST]  "
				+ (l = index.valuesOfRange(Topic.bundle("type1", TopicKind.AAA), Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.equals(Arrays.asList(t1, t2)) || l.equals(Arrays.asList(t2, t1)));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only types ..3:");
		System.err.println("[TEST]  " + (l = index.valuesOfRange(null, Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.equals(Arrays.asList(t1, t2)) || l.equals(Arrays.asList(t2, t1)));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only types 2..:");
		System.err.println("[TEST]  " + (l = index.valuesOfRange(Topic.bundle("type2", TopicKind.AAA), null)));
		assertTrue(l.equals(Arrays.asList(t3, t4)) || l.equals(Arrays.asList(t4, t3)));
		System.err.println("-----------------------------------------------");
		store.update("003", new Updater<Topic>()
		{
			@Override
			public boolean update(Topic value)
			{
				value.objId = "type0";
				return true;
			}
		});
		System.err.println("[TEST]  " + "-- Only type3 After removal 1:");
		System.err.println("[TEST]  " + (l = index.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertEquals(l, Arrays.asList(t4));
		System.err.println("-----------------------------------------------");
		store.putValue(new Topic("004", "type0", TopicKind.AAA));
		System.err.println("[TEST]  " + "-- Only type3 After removal 2:");
		System.err.println("[TEST]  " + (l = index.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.size() == 0);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (t = store.get("001")));
		assertEquals(t, t1);
		System.err.println("-----------------------------------------------");
	}

	// ///////////////////////////////////////////////////////////////////////////////////////////////

	@Test
	public void testDateRanges()
	{
		KeyValueStore<String, Stopover> store;
		Index<String, Stopover, Date> index;

		store = createStore(Stopover.class, new KeyMapper<String, Stopover>()
		{
			@Override
			public String getKeyOf(Stopover stopover)
			{
				return stopover.stpid;
			}
		});

		index = store.createTypedIndex("fET", Date.class, new IndexMapper<Stopover, Date>()
		{
			@Override
			public Date getIndexedFieldOf(Stopover stopover)
			{
				Date executionTime = stopover.arrivalTime;
				if (executionTime == null)
				{
					executionTime = stopover.eta;
					if (executionTime == null)
						executionTime = stopover.notBefore;
				}
				return executionTime;
			}
		});

		final long now = System.currentTimeMillis();

		final Date time1 = new Date(now - 2L);
		final Date time2 = new Date(now - 1L);
		final Date time3 = new Date(now);
		final Date time4 = new Date(now + 1L);
		final Date time5 = new Date(now + 2L);
		final Date timeXX = new Date(0L);

		final Stopover stp1 = new Stopover("stp1", time1, null, null);
		final Stopover stp2 = new Stopover("stp2", null, null, time2);
		final Stopover stp3 = new Stopover("stp3", timeXX, timeXX, timeXX); // incorrect, will be fixed immediately
		final Stopover stp4 = new Stopover("stp4", time4, null, null);
		final Stopover stp5 = new Stopover("stp5", null, null, time5);

		store.addValue(stp1);
		store.addValue(stp2);
		store.addValue(stp3);
		store.addValue(stp4);
		store.addValue(stp5);

		store.update("stp3", new Updater<Stopover>()
		{
			@Override
			public boolean update(Stopover stp3)
			{
				stp3.arrivalTime = null;
				stp3.eta = time3;
				return true;
			}
		});

		store.clearCache();

		List<Stopover> list;

		list = index.valuesOfRange(time3, null);
		assertTrue(list.size() == 3 && list.contains(stp3) && list.contains(stp4) && list.contains(stp5));
		System.err.println("3..  " + list);

		list = index.valuesOfRange(null, time3);
		assertTrue(list.size() == 2 && list.contains(stp1) && list.contains(stp2));
		System.err.println("..3  " + list);

		list = index.valuesOfRange(time2, time4);
		assertTrue(list.size() == 2 && list.contains(stp2) && list.contains(stp3));
		System.err.println("2..4 " + list);

		list = index.valuesOfRange(null, time1);
		assertTrue(list.size() == 0);
		System.err.println("..1  " + list);
	}

}
