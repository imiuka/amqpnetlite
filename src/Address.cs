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
    using Amqp.Types;

    public sealed class Address
    {
        const string Amqp = "AMQP";
        const string Amqps = "AMQPS";
        const int AmqpPort = 5672;
        const int AmqpsPort = 5671;

        public Address(string address)
        {
            this.Port = -1;
            this.Path = "/";
            this.Parse(address);
            this.SetDefault();
        }

        public string Scheme
        {
            get;
            private set;
        }

        public bool UseSsl
        {
            get;
            private set;
        }

        public string Host
        {
            get;
            private set;
        }

        public int Port
        {
            get;
            private set;
        }

        public string User
        {
            get;
            private set;
        }

        public string Password
        {
            get;
            private set;
        }

        public string Path
        {
            get;
            private set;
        }

        enum ParseState
        {
            Scheme,
            Slash1,
            Slash2,
            User,
            Password,
            Host,
            Port,
            Path
        }

        void Parse(string address)
        {
            //  amqp[s]://user:password@a.contoso.com:port/foo/bar
            ParseState state = ParseState.Scheme;
            int startIndex = 0;
            for (int i = 0; i < address.Length; ++i)
            {
                switch (address[i])
                {
                    case ':':
                        if (state == ParseState.Scheme)
                        {
                            this.Scheme = address.Substring(startIndex, i - startIndex);
                            state = ParseState.Slash1;
                        }
                        else if (state == ParseState.User)
                        {
                            this.User = address.Substring(startIndex, i - startIndex);
                            state = ParseState.Password;
                            startIndex = i + 1;
                        }
                        else if (state == ParseState.Host)
                        {
                            this.Host = address.Substring(startIndex, i - startIndex);
                            state = ParseState.Port;
                            startIndex = i + 1;
                        }
                        else
                        {
                            throw new AmqpException(ErrorCode.InvalidField,
                                Fx.Format(SRAmqp.InvalidAddressFormat));
                        }
                        break;
                    case '/':
                        if (state == ParseState.Slash1)
                        {
                            state = ParseState.Slash2;
                        }
                        else if (state == ParseState.Slash2)
                        {
                            state = ParseState.User;
                            startIndex = i + 1;
                        }
                        else if (state == ParseState.User || state == ParseState.Host)
                        {
                            this.Host = address.Substring(startIndex, i - startIndex);
                            state = ParseState.Path;
                            startIndex = i;
                        }
                        else if (state == ParseState.Port)
                        {
                            this.Port = int.Parse(address.Substring(startIndex, i - startIndex));
                            state = ParseState.Path;
                            startIndex = i;
                        }
                        else if (state == ParseState.Password)
                        {
                            this.Host = this.User;
                            this.User = null;
                            this.Port = int.Parse(address.Substring(startIndex, i - startIndex));
                            state = ParseState.Path;
                            startIndex = i;
                        }
                        break;
                    case '@':
                        if (state == ParseState.Password)
                        {
                            this.Password = address.Substring(startIndex, i - startIndex);
                            state = ParseState.Host;
                            startIndex = i + 1;
                        }
                        else
                        {
                            throw new AmqpException(ErrorCode.InvalidField,
                                Fx.Format(SRAmqp.InvalidAddressFormat));
                        }
                        break;
                    default:
                        break;
                }

                if (state == ParseState.Path)
                {
                    this.Path = address.Substring(startIndex);
                    break;
                }
            }

            if (state == ParseState.User || state == ParseState.Host)
            {
                this.Host = address.Substring(startIndex);
            }
            else if (state == ParseState.Port)
            {
                this.Port = int.Parse(address.Substring(startIndex));
            }
        }

        void SetDefault()
        {
            string schemeUpper = this.Scheme.ToUpper();
            if (schemeUpper == Amqps)
            {
                this.UseSsl = true;
            }
            else if (schemeUpper != Amqp)
            {
                throw new AmqpException(ErrorCode.InvalidField, "scheme");
            }

            if (this.Port == -1)
            {
                this.Port = this.UseSsl ? AmqpsPort : AmqpPort;
            }
        }
    }
}