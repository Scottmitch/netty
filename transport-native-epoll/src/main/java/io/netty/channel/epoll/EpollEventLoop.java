/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.epoll;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.epoll.AbstractEpollChannel.AbstractEpollUnsafe;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.IovArray;
import io.netty.util.IntSupplier;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.ScheduledRunnable;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

/**
 * {@link EventLoop} which uses epoll under the covers. Only works on Linux!
 */
class EpollEventLoop extends SingleThreadEventLoop {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(EpollEventLoop.class);

    /**
     * This must be the maximum value for the deadline's domain, which is currently {@link Long#MAX_VALUE}.
     */
    private static final long MAXIMUM_DEADLINE = Long.MAX_VALUE;
    /**
     * This must be the minimum value for the deadline's domain, which is currently {@code 0}.
     */
    private static final long DEADLINE_EVENTLOOP_PROCESSING = 0;
    /**
     * See <a href="http://man7.org/linux/man-pages/man2/timerfd_create.2.html">timerfd_create(2)</a>
     */
    private static final long MAX_SCHEDULED_TIMERFD_NS = 999999999;

    static {
        // Ensure JNI is initialized by the time this class is loaded by this time!
        // We use unix-common methods in this class which are backed by JNI methods.
        Epoll.ensureAvailability();
    }

    /**
     * Note that we use deadline instead of delay because deadline is just a fixed number but delay requires interacting
     * with the time source (e.g. calling {@link System#nanoTime()}) which can be expensive.
     * We exploit the fact that the deadline domain currently only includes non-negative numbers.
     * <ul>
     *     <li>{@link #DEADLINE_EVENTLOOP_PROCESSING} - The EventLoop thread is processing tasks. In this state only
     *     the EventLoop thread will interact with {@link #timerFd} after it is done processing.</li>
     *     <li>{@code <0} - The EventLoop thread is processing tasks, and another thread has executed a timer task
     *     that is the next to execute relative to this value. The EventLoop thread will {@link #decodeDeadline(long)}
     *     this value (e.g. negate it) and compare it to this value before processing tasks and the minimum deadline
     *     from the schedule queue to determine the next minimum value, and may arm the timer based upon this value</li>
     *     <li>{@code >0} - Next deadline to expire set by a non-EventLoop thread, or by the EventLoop thread after is
     *     has completed processing tasks.</li>
     * </ul>
     */
    private final AtomicLong nextDeadlineNanos = new AtomicLong(MAXIMUM_DEADLINE);
    private final AtomicBoolean wakenUp = new AtomicBoolean();
    private final FileDescriptor epollFd;
    private final FileDescriptor eventFd;
    private final FileDescriptor timerFd;
    private final IntObjectMap<AbstractEpollChannel> channels = new IntObjectHashMap<AbstractEpollChannel>(4096);
    private final boolean allowGrowing;
    private final EpollEventArray events;

    // These are initialized on first use
    private IovArray iovArray;
    private NativeDatagramPacketArray datagramPacketArray;

    private final SelectStrategy selectStrategy;
    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return epollWaitNow();
        }
    };

    EpollEventLoop(EventLoopGroup parent, Executor executor, int maxEvents,
                   SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler,
                   EventLoopTaskQueueFactory queueFactory) {
        super(parent, executor, false, newTaskQueue(queueFactory), newTaskQueue(queueFactory),
                rejectedExecutionHandler);
        selectStrategy = ObjectUtil.checkNotNull(strategy, "strategy");
        if (maxEvents == 0) {
            allowGrowing = true;
            events = new EpollEventArray(4096);
        } else {
            allowGrowing = false;
            events = new EpollEventArray(maxEvents);
        }
        boolean success = false;
        FileDescriptor epollFd = null;
        FileDescriptor eventFd = null;
        FileDescriptor timerFd = null;
        try {
            this.epollFd = epollFd = Native.newEpollCreate();
            this.eventFd = eventFd = Native.newEventFd();
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call eventfd_read(...).
                Native.epollCtlAdd(epollFd.intValue(), eventFd.intValue(), Native.EPOLLIN | Native.EPOLLET);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e);
            }
            this.timerFd = timerFd = Native.newTimerFd();
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call read(...).
                Native.epollCtlAdd(epollFd.intValue(), timerFd.intValue(), Native.EPOLLIN | Native.EPOLLET);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to add timerFd filedescriptor to epoll", e);
            }
            success = true;
        } finally {
            if (!success) {
                if (epollFd != null) {
                    try {
                        epollFd.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                if (eventFd != null) {
                    try {
                        eventFd.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                if (timerFd != null) {
                    try {
                        timerFd.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
    }

    /**
     * Return a cleared {@link IovArray} that can be used for writes in this {@link EventLoop}.
     */
    IovArray cleanIovArray() {
        if (iovArray == null) {
            iovArray = new IovArray();
        } else {
            iovArray.clear();
        }
        return iovArray;
    }

    /**
     * Return a cleared {@link NativeDatagramPacketArray} that can be used for writes in this {@link EventLoop}.
     */
    NativeDatagramPacketArray cleanDatagramPacketArray() {
        if (datagramPacketArray == null) {
            datagramPacketArray = new NativeDatagramPacketArray();
        } else {
            datagramPacketArray.clear();
        }
        return datagramPacketArray;
    }

    @Override
    protected void executeScheduledRunnable(ScheduledRunnable runnable) {
        // We must execute before setting the timer. Otherwise we may wake up the EventLoop thread and the scheduled
        // task will be missed because it isn't yet in the queue.
        execute(runnable);

        if (!runnable.isCancelled()) {
            try {
                final long deadlineNanos = runnable.deadlineNanos();
                assert deadlineNanos >= DEADLINE_EVENTLOOP_PROCESSING;
                for (;;) {
                    final long pendingDeadline = nextDeadlineNanos.get();
                    if (pendingDeadline == DEADLINE_EVENTLOOP_PROCESSING) {
                        // The EventLoop thread is processing tasks, we shouldn't attempt to set the timerFd and instead
                        // wait for the EventLoop thread to take care of this after it is done processing.
                        if (nextDeadlineNanos.compareAndSet(DEADLINE_EVENTLOOP_PROCESSING,
                                encodeDeadline(deadlineNanos))) {
                            break;
                        }
                    } else if (pendingDeadline < DEADLINE_EVENTLOOP_PROCESSING) {
                        // The EventLoop thread is processing tasks, we shouldn't attempt to set the timerFd and instead
                        // wait for the EventLoop thread to take care of this after it is done processing.
                        final long normalizedPendingDeadline = decodeDeadline(pendingDeadline);
                        if (normalizedPendingDeadline <= deadlineNanos) {
                            // the new timeout is not sooner to expire than the existing deadline, so just verify the
                            // value hasn't changed and bail.
                            if (nextDeadlineNanos.compareAndSet(pendingDeadline, pendingDeadline)) {
                                break;
                            }
                        } else if (nextDeadlineNanos.compareAndSet(pendingDeadline, encodeDeadline(deadlineNanos))) {
                            // the new deadline expires sooner than the existing deadline, set the value so the
                            // EventLoop thread can pick up the new deadline.
                            break;
                        }
                    } else if (deadlineNanos < pendingDeadline) {
                        // The EventLoop is not processing, we should attempt to set the timerFd to wake up the
                        // EventLoop thread at the appropriate time.
                        if (nextDeadlineNanos.compareAndSet(pendingDeadline, deadlineNanos)) {
                            setTimerFdHandleRace(deadlineNanos);
                            break;
                        }
                    } else if (nextDeadlineNanos.compareAndSet(pendingDeadline, pendingDeadline)) {
                        // this deadline is not the next to expire, so lets just bail.
                        break;
                    }
                }
            } catch (IOException cause) {
                throwTimerUncheckedException(cause);
            }
        }
        // else this is a removal of scheduled task and we could attempt to detect if this task was responsible
        // for the next delay, and find the next lowest delay in the queue to re-set the timer. However this
        // is not practical for the following reasons:
        // 1. The data structure is a PriorityQueue, and the scheduled task has not yet been removed. This means
        //    we would have to add/remove the head element to find the "next timeout".
        // 2. We are not on the EventLoop thread, and the PriorityQueue is not thread safe. We could attempt
        //    to do (1) if we are on the EventLoop but when the EventLoop wakes up it checks if the timeout changes
        //    when it is woken up and before it calls epoll_wait again and adjusts the timer accordingly.
        // The result is we wait until we are in the EventLoop and doing the actual removal, and also processing
        // regular polling in the EventLoop too.
    }

    @Override
    protected boolean runAllTasks() {
        return runAllTasks(false);
    }

    private boolean runAllTasks(boolean timerFdFired) {
        final long previousPendingDeadline = nextDeadlineNanos.getAndSet(DEADLINE_EVENTLOOP_PROCESSING);
        assert previousPendingDeadline >= 0;
        final long beforeTaskQueueNextDeadline = nextScheduledTaskDeadlineNanos();
        try {
            // Note the timerFd code depends upon running all the tasks on each event loop run. This is so
            // we can get an accurate "next wakeup time" after the event loop run completes.
            return runScheduledAndExecutorTasks();
        } finally {
            // Don't disarm the timerFd even if there are no more queued tasks. Instead we wait for the timer wakeup on
            // the EventLoop and clear state for the next timer.
            final long afterTaskQueueNextDeadline = nextScheduledTaskDeadlineNanos();
            try {
                for (;;) {
                    final long pendingDeadline = nextDeadlineNanos.get();
                    if (pendingDeadline == DEADLINE_EVENTLOOP_PROCESSING) {
                        // There were no attempts to set the timer from another thread, we should set nextDeadlineNanos
                        // and potentially call set setTimerFd if the new time is less than the previous time, or if
                        // the previously lowest value for the timer already fired.
                        if (afterTaskQueueNextDeadline >= 0 &&
                                (beforeTaskQueueNextDeadline != afterTaskQueueNextDeadline ||
                                        afterTaskQueueNextDeadline < previousPendingDeadline || timerFdFired)) {
                            if (nextDeadlineNanos.compareAndSet(DEADLINE_EVENTLOOP_PROCESSING,
                                    afterTaskQueueNextDeadline)) {
                                setTimerFdHandleRace(afterTaskQueueNextDeadline);
                                break;
                            }
                        } else if (nextDeadlineNanos.compareAndSet(DEADLINE_EVENTLOOP_PROCESSING, MAXIMUM_DEADLINE)) {
                            // The scheduled task which previously set previousPendingDeadline has been executed.
                            // Otherwise we would have seen a different value because the scheduled task code does:
                            // 1. execute(scheduledTask)
                            // 2. nextDeadlineNanos.CAS(scheduledTask.deadline())
                            // if We don't see a changed value in this thread, then any thread that may change the value
                            // in the future is responsible for setting the timer in the future.
                            break;
                        }
                    } else {
                        // Another thread scheduled a timer task, we should set nextDeadlineNanos and potentially call
                        // set setTimerFd if the new time is less than the previous time.
                        final long normalizedPendingDeadline = decodeDeadline(pendingDeadline);
                        if (afterTaskQueueNextDeadline >= 0 &&
                                beforeTaskQueueNextDeadline != afterTaskQueueNextDeadline) {
                            // the previous minimum timer has executed, we have a new minimum deadline.
                            if (afterTaskQueueNextDeadline <= normalizedPendingDeadline) {
                                if (nextDeadlineNanos.compareAndSet(pendingDeadline, afterTaskQueueNextDeadline)) {
                                    setTimerFdHandleRace(afterTaskQueueNextDeadline);
                                    break;
                                }
                            } else if (nextDeadlineNanos.compareAndSet(pendingDeadline, normalizedPendingDeadline)) {
                                setTimerFdHandleRace(normalizedPendingDeadline);
                                break;
                            }
                        // } else comments below
                        // either there was no scheduled task whose deadline justified execution, or all the scheduled
                        // tasks have been executed. either way there is no contribution toward the deadline from the
                        // scheduled task queue. we should consider restoring the previous deadline or setting the
                        // deadline from the value provided by the other thread.
                        } else if (previousPendingDeadline <= normalizedPendingDeadline) {
                            if (nextDeadlineNanos.compareAndSet(pendingDeadline, previousPendingDeadline)) {
                                // other thread's minimum deadline was not sooner than the deadline that existed before
                                if (timerFdFired) {
                                    // just in case the timer spuriously fired, or there is a timing mismatch where the
                                    // timer fired but the deadline didn't expire when compared with the current
                                    // nanoTime() value we re-arm the timer.
                                    setTimerFdHandleRace(previousPendingDeadline);
                                }
                                break;
                            }
                        } else if (nextDeadlineNanos.compareAndSet(pendingDeadline, normalizedPendingDeadline)) {
                            setTimerFdHandleRace(normalizedPendingDeadline);
                            break;
                        }
                    }
                }
            } catch (IOException cause) {
                throwTimerUncheckedException(cause);
            }
        }
    }

    private static long decodeDeadline(long pendingDeadline) {
        return -pendingDeadline;
    }

    private static long encodeDeadline(long pendingDeadline) {
        return -pendingDeadline;
    }

    private void setTimerFdHandleRace(long candidateNextDeadline) throws IOException {
        // We need to serialize calls to setTimerFd to prevent a later deadline racing with an earlier deadline and over
        // writing it.
        synchronized (nextDeadlineNanos) {
            if (nextDeadlineNanos.get() == candidateNextDeadline) {
                setTimerFd(deadlineToDelayNanos(candidateNextDeadline));
            }
            // It is possible that this method is called from outside the EventLoop thread, and since that time the
            // EventLoop thread has changed nextDeadlineNanos to DEADLINE_EVENTLOOP_PROCESSING. In this case the
            // EventLoop thread is responsible for re-arming the timer for the minimum value so it is OK for the
            // outside EventLoop thread to do nothing.
        }
    }

    private void throwTimerUncheckedException(IOException cause) {
        throw new IllegalStateException("Scheduled task executed, but setting the wakeup timer failed. " +
                "The associated task may not execute in a timely manner."
                + (isShuttingDown() ? " Event loop is shutting down" : "") , cause);
    }

    private void setTimerFd(long candidateNextDelayNanos) throws IOException {
        if (candidateNextDelayNanos > 0) {
            final int delaySeconds = (int) min(candidateNextDelayNanos / 1000000000L, Integer.MAX_VALUE);
            final int delayNanos = (int) min(candidateNextDelayNanos - delaySeconds * 1000000000L,
                    MAX_SCHEDULED_TIMERFD_NS);
            Native.timerFdSetTime(timerFd.intValue(), delaySeconds, delayNanos);
        } else {
            // Setting the timer to 0, 0 will disarm it, so we have a few options:
            // 1. Set the timer wakeup to 1ns (1 system call).
            // 2. Use the eventFd to force a wakeup and disarm the timer (2 system calls).
            // For now we are using option (1) because there are less system calls, and we will correctly reset the
            // nextDeadlineNanos state when the EventLoop processes the timer wakeup.
            Native.timerFdSetTime(timerFd.intValue(), 0, 1);
        }
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && !wakenUp.getAndSet(true)) {
            Native.eventFdWrite(eventFd.intValue(), 1L); // write to the evfd which will then wake-up epoll_wait(...)
        }
    }

    @Override
    protected boolean wakesUpForTask(Runnable task) {
        return !(task instanceof ScheduledRunnable) && super.wakesUpForTask(task);
    }

    /**
     * Register the given epoll with this {@link EventLoop}.
     */
    void add(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        int fd = ch.socket.intValue();
        Native.epollCtlAdd(epollFd.intValue(), fd, ch.flags);
        AbstractEpollChannel old = channels.put(fd, ch);

        // We either expect to have no Channel in the map with the same FD or that the FD of the old Channel is already
        // closed.
        assert old == null || !old.isOpen();
    }

    /**
     * The flags of the given epoll was modified so update the registration
     */
    void modify(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        Native.epollCtlMod(epollFd.intValue(), ch.socket.intValue(), ch.flags);
    }

    /**
     * Deregister the given epoll from this {@link EventLoop}.
     */
    void remove(AbstractEpollChannel ch) throws IOException {
        assert inEventLoop();
        int fd = ch.socket.intValue();

        AbstractEpollChannel old = channels.remove(fd);
        if (old != null && old != ch) {
            // The Channel mapping was already replaced due FD reuse, put back the stored Channel.
            channels.put(fd, old);

            // If we found another Channel in the map that is mapped to the same FD the given Channel MUST be closed.
            assert !ch.isOpen();
        } else if (ch.isOpen()) {
            // Remove the epoll. This is only needed if it's still open as otherwise it will be automatically
            // removed once the file-descriptor is closed.
            Native.epollCtlDel(epollFd.intValue(), fd);
        }
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    private static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    @Override
    public int registeredChannels() {
        return channels.size();
    }

    private int epollWaitNow() throws IOException {
        wakenUp.set(false);
        return Native.epollWait(epollFd, events, true);
    }

    private int epollBusyWait() throws IOException {
        wakenUp.set(false);
        return Native.epollBusyWait(epollFd, events);
    }

    @Override
    protected void run() {
        for (;;) {
            try {
                int strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                if (strategy == SelectStrategy.SELECT) {
                    strategy = Native.epollWait(epollFd, events, wakenUp.getAndSet(false));

                    // If another thread executes a task on the EventLoop the order of events is:
                    // 1. insert into task queue
                    // 2. wakenUp.GAS(true)
                    // There is no need to get hasTasks() here because if we see wakenUp==true that means there
                    // are tasks pending and we should immediate poll to get any pending I/O, process
                    // I/O, and then process pending tasks + scheduled tasks.
                    // There is no need to check/reset wakenUp here because on the next iteration of this loop we
                    // will GAS wakenUp state again, and if true that means we will observe the task queue is
                    // non-empty. If the task queue is non-empty we will poll with no delay and process all pending
                    // tasks in the queue (because we always process all pending tasks that are visible on each
                    // wakeup).
                } else if (strategy == SelectStrategy.BUSY_WAIT) {
                    strategy = epollBusyWait();
                } else if (strategy == SelectStrategy.CONTINUE) {
                    continue;
                }

                // assume the timer fired just to be safe in case there was an exception we will re-arm the timer.
                boolean timerFired = true;
                try {
                    timerFired = processReady(events, strategy);
                } finally {
                    runAllTasks(timerFired);
                }
                if (allowGrowing && strategy == events.length()) {
                    //increase the size of the array as we needed the whole space for the events
                    events.increase();
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
            // Always handle shutdown even if the loop processing threw an exception.
            try {
                if (isShuttingDown()) {
                    closeAll();
                    if (confirmShutdown()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }
    }

    /**
     * Visible only for testing!
     */
    void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    private void closeAll() {
        // Using the intermediate collection to prevent ConcurrentModificationException.
        // In the `close()` method, the channel is deleted from `channels` map.
        AbstractEpollChannel[] localChannels = channels.values().toArray(new AbstractEpollChannel[0]);

        for (AbstractEpollChannel ch: localChannels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    // Returns true if a timerFd event was encountered
    private boolean processReady(EpollEventArray events, int ready) {
        boolean timerFired = false;
        for (int i = 0; i < ready; ++i) {
            final int fd = events.fd(i);
            if (fd == timerFd.intValue()) {
                timerFired = true;
            } else if (fd != eventFd.intValue()) {
                final long ev = events.events(i);
                AbstractEpollChannel ch = channels.get(fd);
                if (ch != null) {
                    // Don't change the ordering of processing EPOLLOUT | EPOLLRDHUP / EPOLLIN if you're not 100%
                    // sure about it!
                    // Re-ordering can easily introduce bugs and bad side-effects, as we found out painfully in the
                    // past.
                    AbstractEpollUnsafe unsafe = (AbstractEpollUnsafe) ch.unsafe();

                    // First check for EPOLLOUT as we may need to fail the connect ChannelPromise before try
                    // to read from the file descriptor.
                    // See https://github.com/netty/netty/issues/3785
                    //
                    // It is possible for an EPOLLOUT or EPOLLERR to be generated when a connection is refused.
                    // In either case epollOutReady() will do the correct thing (finish connecting, or fail
                    // the connection).
                    // See https://github.com/netty/netty/issues/3848
                    if ((ev & (Native.EPOLLERR | Native.EPOLLOUT)) != 0) {
                        // Force flush of data as the epoll is writable again
                        unsafe.epollOutReady();
                    }

                    // Check EPOLLIN before EPOLLRDHUP to ensure all data is read before shutting down the input.
                    // See https://github.com/netty/netty/issues/4317.
                    //
                    // If EPOLLIN or EPOLLERR was received and the channel is still open call epollInReady(). This will
                    // try to read from the underlying file descriptor and so notify the user about the error.
                    if ((ev & (Native.EPOLLERR | Native.EPOLLIN)) != 0) {
                        // The Channel is still open and there is something to read. Do it now.
                        unsafe.epollInReady();
                    }

                    // Check if EPOLLRDHUP was set, this will notify us for connection-reset in which case
                    // we may close the channel directly or try to read more data depending on the state of the
                    // Channel and als depending on the AbstractEpollChannel subtype.
                    if ((ev & Native.EPOLLRDHUP) != 0) {
                        unsafe.epollRdHupReady();
                    }
                } else {
                    // We received an event for an fd which we not use anymore. Remove it from the epoll_event set.
                    try {
                        Native.epollCtlDel(epollFd.intValue(), fd);
                    } catch (IOException ignore) {
                        // This can happen but is nothing we need to worry about as we only try to delete
                        // the fd from the epoll set as we not found it in our mappings. So this call to
                        // epollCtlDel(...) is just to ensure we cleanup stuff and so may fail if it was
                        // deleted before or the file descriptor was closed before.
                    }
                }
            }
        }
        return timerFired;
    }

    @Override
    protected void cleanup() {
        try {
            // Set to true as a best effort to prevent eventFd writes from other threads in the future.
            boolean wakeupPending = wakenUp.getAndSet(true);
            // Best effort to process in-flight wakeup writes have been performed prior to closing eventFd.
            while (wakeupPending) {
                try {
                    int count = Native.epollWait(epollFd, events, 1000);
                    if (count == 0) {
                        // We timed-out so assume that the write we're expecting isn't coming
                        break;
                    }
                    for (int i = 0; i < count; i++) {
                        if (events.fd(i) == eventFd.intValue()) {
                            wakeupPending = false;
                            break;
                        }
                    }
                } catch (IOException ignore) {
                    // ignore
                }
            }
            try {
                // A best effort is made above to prevent eventFd from being used by other threads at this point:
                // 1. wakenUp is set to true (prevents other threads from attempting to write to eventFd).
                // 2. epollWait allows 1 second for the eventFd event to be written to.
                // It is possible a thread has done wakenUp.GAS false->true but not yet written to eventFd until after
                // the timeout.
                eventFd.close();
            } catch (IOException e) {
                logger.warn("Failed to close the event fd.", e);
            }
            // Set state to prevent future calls to schedule(..) from touching timerFd which will be freed.
            // Do it in a synchronized block so that no other thread is currently touching the timerFd.
            synchronized (nextDeadlineNanos) {
                nextDeadlineNanos.set(DEADLINE_EVENTLOOP_PROCESSING);
            }
            try {
                timerFd.close();
            } catch (IOException e) {
                logger.warn("Failed to close the timer fd.", e);
            }

            try {
                epollFd.close();
            } catch (IOException e) {
                logger.warn("Failed to close the epoll fd.", e);
            }
        } finally {
            // release native memory
            if (iovArray != null) {
                iovArray.release();
                iovArray = null;
            }
            if (datagramPacketArray != null) {
                datagramPacketArray.release();
                datagramPacketArray = null;
            }
            events.free();
        }
    }
}
