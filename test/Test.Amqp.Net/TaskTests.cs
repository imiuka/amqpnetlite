//  ------------------------------------------------------------------------------------
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

using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Test.Amqp
{
    [TestClass]
    public class TaskTests
    {
        Address address = LinkTests.address;

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            LinkTests.Initialize(context);
        }

        [TestMethod]
        public async Task BasicSendReceiveAsync()
        {
            string testName = "BasicSendReceiveAsync";
            int nMsgs = 50;

            Connection connection = await ConnectionFactory.CreateConnectionAsync(this.address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, "q1");

            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = new Message();
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties["sn"] = i;
                await sender.SendAsync(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, "q1");
            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = await receiver.ReceiveAsync();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.ApplicationProperties["sn"]);
                receiver.Accept(message);
            }

            await sender.CloseAsync();
            await receiver.CloseAsync();
            await session.CloseAsync();
            await connection.CloseAsync();
        }

        [TestMethod]
        public async Task LargeMessageSendReceiveAsync()
        {
            string testName = "LargeMessageSendReceiveAsync";
            int nMsgs = 50;

            Connection connection = await ConnectionFactory.CreateConnectionAsync(this.address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, "q1");

            int messageSize = 100 * 1024;
            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = new Message(new string('D', messageSize));
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties["sn"] = i;
                await sender.SendAsync(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, "q1");
            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = await receiver.ReceiveAsync();
                string value = (string)message.ValueBody.Value;
                Trace.WriteLine(TraceLevel.Information, "receive: {0} body {1}x{2}",
                    message.ApplicationProperties["sn"], value[0], value.Length);
                receiver.Accept(message);
            }

            await sender.CloseAsync();
            await receiver.CloseAsync();
            await session.CloseAsync();
            await connection.CloseAsync();
        }

        [TestMethod]
        public async Task LargeMessageOnMessageCallback()
        {
            string testName = "LargeMessageOnMessageCallback";
            int nMsgs = 50;

            Connection connection = await ConnectionFactory.CreateConnectionAsync(this.address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, "q1");

            int messageSize = 100 * 1024;
            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = new Message(new string('D', messageSize));
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties["sn"] = i;
                sender.Send(message, null, null);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, "q1");
            ManualResetEvent done = new ManualResetEvent(false);
            int count = 0;
            receiver.Start(30, (link, message) =>
            {
                string value = (string)message.ValueBody.Value;
                Trace.WriteLine(TraceLevel.Information, "receive: {0} body {1}x{2}",
                    message.ApplicationProperties["sn"], value[0], value.Length);
                receiver.Accept(message);

                if (++count == nMsgs) done.Set();
            });

            Assert.IsTrue(done.WaitOne(120000));

            connection.Close();
        }

        [TestMethod]
        public async Task CustomTransportConfiguration()
        {
            string testName = "CustomTransportConfiguration";

            ConnectionFactory factory = new ConnectionFactory();
            factory.TCP.NoDelay = true;
            factory.TCP.SendBufferSize = 16 * 1024;
            factory.TCP.SendTimeout = 30000;
            factory.SSL.RemoteCertificateValidationCallback = (a, b, c, d) => true;
            factory.AMQP.MaxFrameSize = 64 * 1024;
            factory.AMQP.HostName = "contoso.com";
            factory.AMQP.ContainerId = "container:" + testName;

            Address sslAddress = new Address("amqps://guest:guest@127.0.0.1:5671");
            Connection connection = await factory.CreateAsync(sslAddress);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, "q1");

            Message message = new Message("custom transport config");
            message.Properties = new Properties() { MessageId = testName };
            await sender.SendAsync(message);

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, "q1");
            Message message2 = await receiver.ReceiveAsync();
            Assert.IsTrue(message2 != null, "no message received");
            receiver.Accept(message2);

            await connection.CloseAsync();
        }

        [TestMethod]
        public async Task WebSocketSendReceiveAsync()
        {
            string testName = "WebSocketSendReceiveAsync";
            // assuming it matches the broker's setup
            Address wsAddress = new Address("ws://guest:guest@localhost:80");
            int nMsgs = 50;

            Connection connection = await ConnectionFactory.CreateConnectionAsync(this.address);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender-" + testName, "q1");

            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = new Message();
                message.Properties = new Properties() { MessageId = "msg" + i, GroupId = testName };
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties["sn"] = i;
                await sender.SendAsync(message);
            }

            ReceiverLink receiver = new ReceiverLink(session, "receiver-" + testName, "q1");
            for (int i = 0; i < nMsgs; ++i)
            {
                Message message = await receiver.ReceiveAsync();
                Trace.WriteLine(TraceLevel.Information, "receive: {0}", message.ApplicationProperties["sn"]);
                receiver.Accept(message);
            }

            await sender.CloseAsync();
            await receiver.CloseAsync();
            await session.CloseAsync();
            await connection.CloseAsync();
        }
    }
}
