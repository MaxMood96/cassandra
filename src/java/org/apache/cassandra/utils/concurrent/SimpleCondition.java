/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils.concurrent;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)
public class SimpleCondition implements Condition
{
    private static final AtomicReferenceFieldUpdater<SimpleCondition, WaitQueue> waitingUpdater = AtomicReferenceFieldUpdater.newUpdater(SimpleCondition.class, WaitQueue.class, "waiting");

    private volatile WaitQueue waiting;
    private volatile boolean signaled = false;

    @Override
    public void await() throws InterruptedException
    {
        if (isSignaled())
            return;
        if (waiting == null)
            waitingUpdater.compareAndSet(this, null, new WaitQueue());
        WaitQueue.Signal s = waiting.register();
        if (isSignaled())
            s.cancel();
        else
            s.await();
        assert isSignaled();
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException
    {
        long start = nanoTime();
        long until = start + unit.toNanos(time);
        return awaitUntil(until);
    }

    public boolean awaitUntil(long deadlineNanos) throws InterruptedException
    {
        if (isSignaled())
            return true;
        if (waiting == null)
            waitingUpdater.compareAndSet(this, null, new WaitQueue());
        WaitQueue.Signal s = waiting.register();
        if (isSignaled())
        {
            s.cancel();
            return true;
        }
        return s.awaitUntil(deadlineNanos) || isSignaled();
    }

    public void signal()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isSignaled()
    {
        return signaled;
    }

    public void signalAll()
    {
        signaled = true;
        if (waiting != null)
            waiting.signalAll();
    }

    public void awaitUninterruptibly()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long awaitNanos(long nanosTimeout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitUntil(Date deadline)
    {
        throw new UnsupportedOperationException();
    }
}
