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

namespace Amqp.Listener
{
    using Amqp.Framing;

    /// <summary>
    /// Provides the context of a receive request. It is created by a message source
    /// to track the delivery of an outgoing message.
    /// </summary>
    public class ReceiveContext : Context
    {
        /// <summary>
        /// Initializes a ReceiveContext object.
        /// </summary>
        /// <param name="link">The listener link to send out the message.</param>
        /// <param name="message">The message to send out.</param>
        public ReceiveContext(ListenerLink link, Message message)
            : base(link, message)
        {
        }

        /// <summary>
        /// Completes a receive request. The message source should override this method
        /// to handle the outcome of the delivery.
        /// </summary>
        /// <param name="deliveryState">Delivery state of the message.</param>
        /// <remarks>
        /// A derived class overrides this method to handle delivery outcome. See
        /// DispositionContext.OnDisposition documentation for details on handling
        /// different outcomes.
        /// </remarks>
        public virtual void Complete(DeliveryState deliveryState)
        {
            this.Dispose(deliveryState);
        }
    }
}
