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

package org.apache.cassandra.cql3.ast;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.StringType;

public class Literal implements Value
{
    private final Object value;
    private final AbstractType<?> type;

    public Literal(Object value, AbstractType<?> type)
    {
        this.value = value;
        this.type = type;
    }

    public static Literal of(int value)
    {
        return new Literal(value, Int32Type.instance);
    }

    @Override
    public AbstractType<?> type()
    {
        return type;
    }

    @Override
    public Object value()
    {
        return value;
    }

    @Override
    public ByteBuffer valueEncoded()
    {
        return value instanceof ByteBuffer ? (ByteBuffer) value : ((AbstractType) type).decompose(value);
    }

    @Override
    public Literal with(Object value, AbstractType<?> type)
    {
        return new Literal(value, type);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        ByteBuffer bytes = valueEncoded();
        if (bytes.remaining() == 0 && !actuallySupportsEmpty(type))
        {
            sb.append("<empty bytes>");
            return;
        }
        sb.append(type.asCQL3Type().toCQLLiteral(bytes));
    }

    private static boolean actuallySupportsEmpty(AbstractType<?> type)
    {
        return type == BytesType.instance || type instanceof StringType;
    }
}
