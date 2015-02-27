package com.tectonica.test;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.tectonica.kvs.Index;
import com.tectonica.kvs.Index.IndexMapper;
import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.KeyValueStore.KeyMapper;
import com.tectonica.kvs.Updater;
import com.tectonica.kvs.impl.GaeKeyValueStore;
import com.tectonica.kvs.impl.GaeKeyValueStore.Config;

public class TestGaeKeyValueStore extends TestKeyValueStore
{
	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig().setDefaultHighRepJobPolicyUnappliedJobPercentage(100),
			new LocalMemcacheServiceTestConfig());

	@Before
	@Override
	public void setUp()
	{
		helper.setUp();
		super.setUp();
	}

	@After
	@Override
	public void tearDown()
	{
		helper.tearDown();
		super.tearDown();
	}

	@Override
	protected KeyValueStore<String, Topic> createStore()
	{
		return new GaeKeyValueStore<>(Config.create(Topic.class).withNamespace("ns"), keyMapper);
	}

	@Test
	@Ignore
	public void test()
	{
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		assertEquals(0, ds.prepare(new Query("yam")).countEntities(withLimit(10)));
		ds.put(new Entity("yam"));
		ds.put(new Entity("yam"));
		assertEquals(2, ds.prepare(new Query("yam")).countEntities(withLimit(10)));

		MemcacheService mc = MemcacheServiceFactory.getMemcacheService();
		assertFalse(mc.contains("yar"));
		mc.put("yar", "foo");
		assertTrue(mc.contains("yar"));
	}

	@SuppressWarnings("serial")
	public static class Stopover implements Serializable
	{
		String stpid;
		Date arrivalTime;
		Date eta;
		Date notBefore;

		public Stopover()
		{}

		public Stopover(String stpid, Date arrivalTime, Date eta, Date notBefore)
		{
			this.stpid = stpid;
			this.arrivalTime = arrivalTime;
			this.eta = eta;
			this.notBefore = notBefore;
		}

		@Override
		public String toString()
		{
			return "[" + stpid + "]";
		}

		@Override
		public boolean equals(Object obj)
		{
			return stpid.equals(((Stopover) obj).stpid);
		}
	}

	@Test
	public void testDateRanges()
	{
		KeyValueStore<String, Stopover> stpStore;
		Index<String, Stopover, Date> stopFromExecTime;

		stpStore = new GaeKeyValueStore<>(Config.create(Stopover.class).withNamespace("STAM"), new KeyMapper<String, Stopover>()
		{
			@Override
			public String getKeyOf(Stopover stopover)
			{
				return stopover.stpid;
			}
		});

		stopFromExecTime = stpStore.createIndex("fET", new IndexMapper<Stopover, Date>()
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

		final Date time1 = new Date(now - 2000L);
		final Date time2 = new Date(now - 1000L);
		final Date time3 = new Date(now);
		final Date time4 = new Date(now + 1000L);
		final Date time5 = new Date(now + 2000L);
		final Date timeXX = new Date(0L);

		final Stopover stp1 = new Stopover("stp1", time1, null, null);
		final Stopover stp2 = new Stopover("stp2", null, null, time2);
		final Stopover stp3 = new Stopover("stp3", timeXX, timeXX, timeXX); // incorrect, will be fixed immediately
		final Stopover stp4 = new Stopover("stp4", time4, null, null);
		final Stopover stp5 = new Stopover("stp5", null, null, time5);

		stpStore.addValue(stp1);
		stpStore.addValue(stp2);
		stpStore.addValue(stp3);
		stpStore.addValue(stp4);
		stpStore.addValue(stp5);

		stpStore.update("stp3", new Updater<Stopover>()
		{
			@Override
			public boolean update(Stopover stp3)
			{
				stp3.arrivalTime = null;
				stp3.eta = time3;
				return true;
			}
		});

		stpStore.clearCache();

		List<Stopover> list;

		list = stopFromExecTime.valuesOfRange(time3, null);
		assertTrue(list.size() == 3 && list.contains(stp3) && list.contains(stp4) && list.contains(stp5));
		System.err.println("3..  " + list);

		list = stopFromExecTime.valuesOfRange(null, time3);
		assertTrue(list.size() == 2 && list.contains(stp1) && list.contains(stp2));
		System.err.println("..3  " + list);

		list = stopFromExecTime.valuesOfRange(time2, time4);
		assertTrue(list.size() == 2 && list.contains(stp2) && list.contains(stp3));
		System.err.println("2..4 " + list);

		list = stopFromExecTime.valuesOfRange(null, time1);
		assertTrue(list.size() == 0);
		System.err.println("..1  " + list);
	}
}
