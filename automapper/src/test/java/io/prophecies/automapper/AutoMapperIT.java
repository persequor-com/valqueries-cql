package io.prophecies.automapper;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.type.codec.ZonedTimestampCodec;
import com.google.inject.Guice;
import io.ran.GenericFactory;
import io.ran.Resolver;
import io.prophecies.Cassandra;
import io.prophecies.ICassandraConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AutoMapperIT extends AutoMapperBaseTests {
	private static Cassandra initDb;
	private Cassandra database;


	@Override
	protected void setInjector() {
		try {
			initDb = new Cassandra(new CqlSessionBuilder()
						.addContactPoint(new InetSocketAddress(InetAddress.getByName("0.0.0.0"),9042))
						.withLocalDatacenter("datacenter1")
						.withAuthCredentials("cassandra", "cassandra")
						.build()
					, new TestCassandraConfig());
			initDb.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", r -> {});
			database = new Cassandra(new CqlSessionBuilder()
					.addContactPoint(new InetSocketAddress(InetAddress.getByName("0.0.0.0"),9042))
					.withLocalDatacenter("datacenter1")
					.withAuthCredentials("cassandra", "cassandra")
					.withKeyspace("test")
					.addTypeCodecs(new ZonedTimestampCodec())
					.build()
					, new TestCassandraConfig());
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		GuiceModule module = new GuiceModule(database, PropheciesResolverImpl.class);
		injector = Guice.createInjector(module);
		factory = injector.getInstance(GenericFactory.class);
	}

	@Before
	public void setup() {


		database.execute(sqlGenerator.generate(carDescriber), r -> {});
		database.execute("TRUNCATE TABLE "+sqlGenerator.getTableName(carDescriber)+";", r -> {});
		database.execute(sqlGenerator.generate(doorDescriber), r -> {});
		database.execute("TRUNCATE TABLE "+sqlGenerator.getTableName(doorDescriber)+";", r -> {});
		database.execute(sqlGenerator.generate(engineDescriber), r -> {});
		database.execute("TRUNCATE TABLE "+sqlGenerator.getTableName(engineDescriber)+";", r -> {});
		database.execute(sqlGenerator.generate(engineCarDescriber), r -> {});
		database.execute("TRUNCATE TABLE "+sqlGenerator.getTableName(engineCarDescriber)+";", r -> {});
		database.execute(sqlGenerator.generate(exhaustDescriber), r -> {});
		database.execute("TRUNCATE TABLE "+sqlGenerator.getTableName(exhaustDescriber)+";", r -> {});
	}

	@After
	public void cleanup() {

	}

	@Test
	public void eagerLoad() throws Throwable {
		Exhaust exhaust = factory.get(Exhaust.class);
		exhaust.setId(UUID.randomUUID());
		exhaust.setBrand(Brand.Porsche);
		exhaustRepository.save(exhaust);

		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaust(exhaust);
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		Collection<Car> cars = carRepository.getEager(model.getId());
		Class<? extends Car> cl = cars.stream().findFirst().get().getClass();
		cl.getMethod("_resolverInject", Resolver.class).invoke(cars.stream().findFirst().get(), resolver);

		assertEquals(1, cars.size());
		Exhaust exhaust1 = cars.stream().findFirst().get().getExhaust();
		assertEquals(exhaust.getId(), exhaust1.getId());

		verifyNoInteractions(resolver);
	}

	private class TestCassandraConfig implements ICassandraConfig {
		@Override
		public int getMaxSaltValue() {
			return 1000;
		}

		@Override
		public int getPartitionBatchSize() {
			return 1000;
		}

		@Override
		public int getMaxActiveAsyncFuturesPerThread() {
			return 1000;
		}

		@Override
		public int getAsyncStatementTimeOutInSec() {
			return 20;
		}
	}
}
