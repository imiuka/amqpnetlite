﻿//  ------------------------------------------------------------------------------------
//  Copyright (c) Microsoft Corporation
//  All rights reserved. 
//  
//  Licensed under the Apache License, Version 2.0 (the ""License""); you may not use this 
//  file except in compliance with the License. You may obtain a copy of the License at 
//  http://www.apache.org/licenses/LICENSE-2.0  
//  
//  THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
//  EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR 
//  CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR 
//  NON-INFRINGEMENT. 
// 
//  See the Apache Version 2.0 License for specific language governing permissions and 
//  limitations under the License.
//  ------------------------------------------------------------------------------------

namespace Amqp
{
    using Amqp.Framing;
    using Amqp.Types;
    using System.Threading;

    public delegate void MessageCallback(ReceiverLink receiver, Message message);

    public sealed class ReceiverLink : Link
    {
#if DOTNET
        const int DefaultCredit = 200;
#else
        const int DefaultCredit = 20;
#endif
        // flow control
        SequenceNumber deliveryCount;
        int totalCredit;
        int credit;
        int restored;

        // received messages queue
        LinkedList receivedMessages;
        Delivery deliveryCurrent;

        // pending receivers
        LinkedList waiterList;
        MessageCallback onMessage;

        public ReceiverLink(Session session, string name, string adderss)
            : this(session, name, new Source() { Address = adderss })
        {
        }

        public ReceiverLink(Session session, string name, Source source)
            : this(session, name, source, null)
        {
        }

        public ReceiverLink(Session session, string name, Source source, OnAttached onAttached)
            : base(session, name, onAttached)
        {
            this.totalCredit = -1;
            this.receivedMessages = new LinkedList();
            this.waiterList = new LinkedList();
            this.SendAttach(true, 0, new Target(), source);
        }

        public void Start(int credit, MessageCallback onMessage = null)
        {
            this.onMessage = onMessage;
            this.SetCredit(credit, true);
        }

        public void SetCredit(int credit, bool autoRestore = true)
        {
            uint dc;
            lock (this.ThisLock)
            {
                if (this.IsDetaching)
                {
                    return;
                }

                this.totalCredit = autoRestore ? credit : 0;
                this.credit = credit;
                this.restored = 0;
                dc = this.deliveryCount;
            }

            this.SendFlow(dc, (uint)credit);
        }

        public Message Receive(int timeout = 60000)
        {
            return this.ReceiveInternal(null, timeout);
        }

#if DOTNET
        public Message Receive(MessageCallback callback, int timeout = 60000)
        {
            return this.ReceiveInternal(callback, timeout);
        }
#endif

        public void Accept(Message message)
        {
            this.ThrowIfDetaching("Accept");
            this.DisposeMessage(message, new Accepted());
        }

        public void Release(Message message)
        {
            this.ThrowIfDetaching("Release");
            this.DisposeMessage(message, new Released());
        }

        public void Reject(Message message, Error error = null)
        {
            this.ThrowIfDetaching("Reject");
            this.DisposeMessage(message, new Rejected() { Error = error });
        }

        internal override void OnFlow(Flow flow)
        {
        }

        internal override void OnTransfer(Delivery delivery, Transfer transfer, ByteBuffer buffer)
        {
            if (!transfer.More)
            {
                Waiter waiter;
                MessageCallback callback;
                lock (this.ThisLock)
                {
                    if (delivery == null)
                    {
                        // multi-transfer delivery
                        delivery = this.deliveryCurrent;
                        this.deliveryCurrent = null;
                        Fx.Assert(delivery != null, "Must have a delivery in the queue");
                        AmqpBitConverter.WriteBytes(delivery.Buffer, buffer.Buffer, buffer.Offset, buffer.Length);
                        delivery.Message = new Message().Decode(delivery.Buffer);
                        delivery.Buffer = null;
                    }
                    else
                    {
                        // single tranfer delivery
                        this.OnDelivery(transfer.DeliveryId);
                        delivery.Message = new Message().Decode(buffer);
                    }

                    callback = this.onMessage;
                    waiter = (Waiter)this.waiterList.First;
                    if (waiter != null)
                    {
                        this.waiterList.Remove(waiter);
                    }

                    if (waiter == null && callback == null)
                    {
                        this.receivedMessages.Add(new MessageNode() { Message = delivery.Message });
                        return;
                    }
                }

                while (waiter != null)
                {
                    if (waiter.Signal(delivery.Message))
                    {
                        this.OnDeliverMessage();
                        return;
                    }

                    lock (this.ThisLock)
                    {
                        waiter = (Waiter)this.waiterList.First;
                        if (waiter != null)
                        {
                            this.waiterList.Remove(waiter);
                        }
                        else if (callback == null)
                        {
                            this.receivedMessages.Add(new MessageNode() { Message = delivery.Message });
                            return;
                        }
                    }
                }

                Fx.Assert(waiter == null, "waiter must be null now");
                Fx.Assert(callback != null, "callback must not be null now");
                callback(this, delivery.Message);
                this.OnDeliverMessage();
            }
            else
            {
                lock (this.ThisLock)
                {
                    if (delivery == null)
                    {
                        delivery = this.deliveryCurrent;
                        Fx.Assert(delivery != null, "Must have a current delivery");
                        AmqpBitConverter.WriteBytes(delivery.Buffer, buffer.Buffer, buffer.Offset, buffer.Length);
                    }
                    else
                    {
                        this.OnDelivery(transfer.DeliveryId);
                        delivery.Buffer = new ByteBuffer(buffer.Length * 2, true);
                        AmqpBitConverter.WriteBytes(delivery.Buffer, buffer.Buffer, buffer.Offset, buffer.Length);
                        this.deliveryCurrent = delivery;
                    }
                }
            }
        }

        internal override void HandleAttach(Attach attach)
        {
            this.deliveryCount = attach.InitialDeliveryCount;
        }

        internal override void OnDeliveryStateChanged(Delivery delivery)
        {
        }

        protected override bool OnClose(Error error)
        {
            Waiter waiter;
            lock (this.ThisLock)
            {
                waiter = (Waiter)this.waiterList.Clear();
            }

            while (waiter != null)
            {
                waiter.Signal(null);
                waiter = (Waiter)waiter.Next;
            }

            return base.OnClose(error);
        }

        void OnDeliverMessage()
        {
            if (this.totalCredit > 0 &&
                Interlocked.Increment(ref this.restored) >= (this.totalCredit / 2))
            {
                this.SetCredit(this.totalCredit, true);
            }
        }

        Message ReceiveInternal(MessageCallback callback, int timeout = 60000)
        {
            this.ThrowIfDetaching("Receive");
            if (this.totalCredit < 0)
            {
                this.SetCredit(DefaultCredit, true);
            }

            Waiter waiter = null;
            lock (this.ThisLock)
            {
                MessageNode first = (MessageNode)this.receivedMessages.First;
                if (first != null)
                {
                    this.receivedMessages.Remove(first);
                    this.OnDeliverMessage();
                    return first.Message;
                }

                if (timeout == 0)
                {
                    return null;
                }

#if DOTNET
                waiter = callback == null ? (Waiter)new SyncWaiter() : new AsyncWaiter(this, callback);
#else
                waiter = new SyncWaiter();
#endif
                this.waiterList.Add(waiter);
            }

            return waiter.Wait(timeout);
        }
        
        void DisposeMessage(Message message, Outcome outcome)
        {
            Delivery delivery = message.Delivery;
            if (delivery == null || delivery.Settled)
            {
                return;
            }

            DeliveryState state = outcome;
            bool settled = true;
#if DOTNET
            var txnState = Amqp.Transactions.ResourceManager.GetTransactionalStateAsync(this).Result;
            if (txnState != null)
            {
                txnState.Outcome = outcome;
                state = txnState;
                settled = false;
            }
#endif
            this.Session.DisposeDelivery(true, delivery, state, settled);
        }

        void OnDelivery(SequenceNumber deliveryId)
        {
            // called with lock held
            if (this.credit <= 0)
            {
                throw new AmqpException(ErrorCode.TransferLimitExceeded,
                    Fx.Format(SRAmqp.DeliveryLimitExceeded, deliveryId));
            }

            this.deliveryCount++;
            this.credit--;
        }

        sealed class MessageNode : INode
        {
            public Message Message { get; set; }

            public INode Previous { get; set; }

            public INode Next { get; set; }
        }

        abstract class Waiter : INode
        {
            public INode Previous { get; set; }

            public INode Next { get; set; }

            public abstract Message Wait(int timeout);

            public abstract bool Signal(Message message);
        }

        sealed class SyncWaiter : Waiter
        {
            readonly ManualResetEvent signal;
            Message message;
            bool expired;

            public SyncWaiter()
            {
                this.signal = new ManualResetEvent(false);
            }

            public override Message Wait(int timeout)
            {
                this.signal.WaitOne(timeout, false);
                lock (this)
                {
                    this.expired = this.message == null;
                    return this.message;
                }
            }

            public override bool Signal(Message message)
            {
                bool signaled = false;
                lock (this)
                {
                    if (!this.expired)
                    {
                        this.message = message;
                        signaled = true;
                    }
                }

                this.signal.Set();
                return signaled;
            }
        }

#if DOTNET
        sealed class AsyncWaiter : Waiter
        {
            readonly static TimerCallback onTimer = OnTimer;
            readonly ReceiverLink link;
            readonly MessageCallback callback;
            Timer timer;
            int state;  // 0: created, 1: waiting, 2: signaled

            public AsyncWaiter(ReceiverLink link, MessageCallback callback)
            {
                this.link = link;
                this.callback = callback;
            }

            public override Message Wait(int timeout)
            {
                this.timer = new Timer(onTimer, this, timeout, -1);
                if (Interlocked.CompareExchange(ref this.state, 1, 0) != 0)
                {
                    // already signaled
                    this.timer.Dispose();
                }

                return null;
            }

            public override bool Signal(Message message)
            {
                int old = Interlocked.Exchange(ref this.state, 2);
                if (old != 2)
                {
                    if (old == 1)
                    {
                        this.timer.Dispose();
                    }

                    this.callback(this.link, message);
                    return true;
                }
                else
                {
                    return false;
                }
            }

            static void OnTimer(object state)
            {
                var thisPtr = (AsyncWaiter)state;
                thisPtr.Signal(null);
            }
        }
#endif
    }
}