package io.prophecies;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;

public class CassandraSettableData implements ICassandraSettableData<CassandraSettableData> {

	private BoundStatement innerSettable;

	public CassandraSettableData(BoundStatement innerSettable) {
		this.innerSettable = innerSettable;
	}

	@Override
	public CassandraSettableData setZonedDateTime(String name, ZonedDateTime value) {
		innerSettable = innerSettable.set(name, value, ZonedDateTime.class);
		return this;
	}

	@Override
	public int firstIndexOf(@NonNull String s) {
		return innerSettable.firstIndexOf(s);
	}

	@NonNull
	@Override
	public CassandraSettableData setBytesUnsafe(int i, @Nullable ByteBuffer byteBuffer) {
		innerSettable = innerSettable.setBytesUnsafe(i, byteBuffer);
		return this;
	}

	@Override
	public int size() {
		return innerSettable.size();
	}

	@NonNull
	@Override
	public DataType getType(int i) {
		return innerSettable.getType(i);
	}

	@NonNull
	@Override
	public CodecRegistry codecRegistry() {
		return innerSettable.codecRegistry();
	}

	@NonNull
	@Override
	public ProtocolVersion protocolVersion() {
		return innerSettable.protocolVersion();
	}

	public BoundStatement unwrapStatement() {
		return innerSettable;
	}
}
