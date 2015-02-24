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

public abstract class Updater<V>
{
	boolean stopped = false;
	boolean changed = false;

	/**
	 * method inside which an entry may be safely modified with no concurrency or shared-state concerns
	 * 
	 * @param value
	 *            thread-safe entry on which the modifications are to be performed. never a null.
	 * @return
	 *         true if the method execution indeed changed the entry
	 */
	public abstract boolean update(V value);

	/**
	 * executes after the persistence has happened and getters return the new value. however, the entry is still locked at that point
	 * and can be addressed without any concern that another updater tries to start a modification.
	 * <p>
	 * IMPORTANT: do not make any modifications to the passed entry inside this method
	 */
	public void postPersist(V value)
	{}

	/**
	 * called if an update process was requested on a non existing key
	 */
	public void entryNotFound()
	{}

	/**
	 * when updating a bulk (e.g. {@link KeyValueStore#updateAll(Updater)}), call this method to stop iteration
	 */
	protected final void stopIteration()
	{
		stopped = true;
	}

	/**
	 * returns whether the last invocation of {@link #update(Object)} changed the entry
	 */
	public final boolean isChanged()
	{
		return changed;
	}
}
