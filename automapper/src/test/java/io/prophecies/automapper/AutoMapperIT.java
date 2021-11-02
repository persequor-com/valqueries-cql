package io.prophecies.automapper;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.type.codec.ZonedTimestampCodec;
import com.google.inject.Guice;
import io.ran.CrudRepository;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
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
		sqlGenerator.generate(carDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(carDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));

		sqlGenerator.generate(doorDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(doorDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));

		sqlGenerator.generate(engineDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(engineDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));

		sqlGenerator.generate(engineCarDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(engineCarDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));

		sqlGenerator.generate(exhaustDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(exhaustDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));

		sqlGenerator.generate(tireDescriber).forEach(cql -> database.execute(cql, r -> {}));
		sqlGenerator.getTableNames(tireDescriber).forEach(tableName -> database.execute("TRUNCATE TABLE "+tableName+";", r -> {}));
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


	@Test
	public void eagerLoad_multiple() throws Throwable {
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

		Door lazyModel = factory.get(Door.class);
		lazyModel.setId(UUID.randomUUID());
		lazyModel.setTitle("Lazy as such");
		lazyModel.setCar(model);
		doorRepository.save(lazyModel);

		Door lazyModelToo = factory.get(Door.class);
		lazyModelToo.setId(UUID.randomUUID());
		lazyModelToo.setTitle("Lazy as well");
		lazyModelToo.setCar(model);
		doorRepository.save(lazyModelToo);

		Collection<Car> cars = carRepository.query().withEager(Car::getDoors).withEager(Car::getExhaust).eq(Car::getId, model.getId()).execute().collect(Collectors.toList());

		Class<? extends Car> cl = cars.stream().findFirst().get().getClass();
		cl.getMethod("_resolverInject", Resolver.class).invoke(cars.stream().findFirst().get(), resolver);
		verifyNoInteractions(resolver);

		assertEquals(1, cars.size());
		List<Door> doors = cars.stream().findFirst().get().getDoors();
		assertEquals(2, doors.size());
		Exhaust actualExhaust = cars.stream().findFirst().get().getExhaust();
		assertEquals(exhaust.getId(), actualExhaust.getId());
	}

	@Test
	public void eagerLoad_fromCompoundKey() throws Throwable {
		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		Tire tire = factory.get(Tire.class);
		tire.setCar(model);
		tire.setBrand(Brand.Porsche);
		tireRepository.save(tire);

		Tire res = tireRepository.query()
				.eq(Tire::getBrand, Brand.Porsche)
				.eq(Tire::getCarId, model.getId())
				.withEager(Tire::getCar)
				.execute().findFirst().orElseThrow(() -> new RuntimeException());

		res.getClass().getMethod("_resolverInject", Resolver.class).invoke(res, resolver);

		Car actual = res.getCar();
		assertNotNull(actual);

		verifyNoInteractions(resolver);
	}

	@Test
	public void subQuery() throws Throwable {
		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		Door door = factory.get(Door.class);
		door.setId(UUID.randomUUID());
		door.setTitle("Lazy as such");
		door.setCar(model);
		doorRepository.save(door);

		Door res = doorRepository.query()
				.subQuery(Door::getCar, sq -> {
					sq.eq(Car::getId, model.getId());
				})
				.execute().findFirst().orElseThrow(() -> new RuntimeException());

		assertEquals(door.getId(), res.getId());

		Car carRes = carRepository.query()
			.subQueryList(Car::getDoors, sq -> {
				sq.eq(Door::getId, door.getId());
			})
			.execute().findFirst().orElseThrow(() -> new RuntimeException());

		assertEquals(model.getId(), carRes.getId());
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
