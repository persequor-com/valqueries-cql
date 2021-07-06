package io.prophecies;

public interface ICassandraConfig {
	int getMaxSaltValue();
	int getPartitionBatchSize();
	int getMaxActiveAsyncFuturesPerThread();
	int getAsyncStatementTimeOutInSec();
}
