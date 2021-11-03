package io.prophecies.automapper;

import com.google.inject.Injector;
import io.ran.AutoMapper;
import io.ran.GenericFactory;
import io.ran.Resolver;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;
import io.ran.AutoMapperClassLoader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public abstract class AutoMapperBaseTests {
	static Injector injector;
	static GenericFactory factory;
	static AutoMapperClassLoader classLoader;
	static TypeDescriber<Car> carDescriber;
	static TypeDescriber<Door> doorDescriber;
	static TypeDescriber<Engine> engineDescriber;
	static TypeDescriber<EngineCar> engineCarDescriber;
	static TypeDescriber<Tire> tireDescriber;
	static AutoMapper mapper;
	@Mock
	Resolver resolver;
	CarRepository carRepository;
	DoorRepository doorRepository;
	CqlGenerator sqlGenerator;
	private EngineRepository engineRepository;
	TypeDescriber<Exhaust> exhaustDescriber;
	ExhaustRepository exhaustRepository;
	TireRepository tireRepository;

	@BeforeClass
	public static void setupClass() {

	}

	@Before
	public void setupBase() {
		setInjector();
		sqlGenerator = new CqlGenerator();
		carRepository = injector.getInstance(CarRepository.class);
		doorRepository = injector.getInstance(DoorRepository.class);
		engineRepository = injector.getInstance(EngineRepository.class);
		exhaustRepository = injector.getInstance(ExhaustRepository.class);
		tireRepository = injector.getInstance(TireRepository.class);
		carDescriber = TypeDescriberImpl.getTypeDescriber(Car.class);
		carDescriber.relations();
		doorDescriber = TypeDescriberImpl.getTypeDescriber(Door.class);
		engineDescriber = TypeDescriberImpl.getTypeDescriber(Engine.class);
		engineCarDescriber = TypeDescriberImpl.getTypeDescriber(EngineCar.class);
		exhaustDescriber = TypeDescriberImpl.getTypeDescriber(Exhaust.class);
		tireDescriber = TypeDescriberImpl.getTypeDescriber(Tire.class);
	}

	protected abstract void setInjector();

	@Test
	public void happy() {
		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setBrand(Brand.Porsche);
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		Optional<Car> actualOptional = carRepository.get(model.getId());
		Car actual = actualOptional.orElseThrow(RuntimeException::new);
		assertEquals(model.getId(), actual.getId());
		assertEquals(model.getTitle(), actual.getTitle());
		assertEquals(model.getCreatedAt().withZoneSameInstant(ZoneOffset.UTC), actual.getCreatedAt().withZoneSameInstant(ZoneOffset.UTC));
		assertEquals(Brand.Porsche, actual.getBrand());
	}

	@Test
	public void lazyLoad() {
		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		Door lazyModel = factory.get(Door.class);
		lazyModel.setId(UUID.randomUUID());
		lazyModel.setTitle("Lazy as such");
		lazyModel.setCar(model);
		doorRepository.save(lazyModel);

		Door actualLazy = doorRepository.get(lazyModel.getId()).orElseThrow(RuntimeException::new);
		Car actual = actualLazy.getCar();
		assertEquals(model.getId(), actual.getId());
		assertEquals(model.getTitle(), actual.getTitle());
		assertEquals(model.getCreatedAt().toInstant(), actual.getCreatedAt().toInstant());
	}

	@Test
	public void queryBuilder() {
		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);

		model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh 2");
		model.setExhaustId(UUID.randomUUID());
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		carRepository.save(model);


		Collection<Car> cars = carRepository.query().eq(Car::getId,model.getId()).execute().collect(Collectors.toList());
		assertEquals(1, cars.size());
	}


	@Test
	public void saveRelationsThatAreMarkedSo() throws Throwable {
		Exhaust exhaust = factory.get(Exhaust.class);
		exhaust.setId(UUID.randomUUID());
		exhaust.setBrand(Brand.Porsche);

		Door door1 = factory.get(Door.class);
		door1.setId(UUID.randomUUID());
		door1.setTitle("Lazy as such");

		Door door2 = factory.get(Door.class);
		door2.setId(UUID.randomUUID());
		door2.setTitle("Lazy as well");

		Car model = factory.get(Car.class);
		model.setId(UUID.randomUUID());
		model.setTitle("Muh");
		model.setExhaust(exhaust);
		model.setCreatedAt(ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
		model.setDoors(Arrays.asList(door1, door2));
		carRepository.save(model);

		Optional<Door> actualDoor1 = doorRepository.get(door1.getId());
		assertTrue(actualDoor1.isPresent());
		assertEquals(door1.getTitle(), actualDoor1.get().getTitle());

		Optional<Door> actualDoor2 = doorRepository.get(door2.getId());
		assertTrue(actualDoor2.isPresent());
		assertEquals(door2.getTitle(), actualDoor2.get().getTitle());

		Optional<Exhaust> actualExhaust = exhaustRepository.get(exhaust.getId());
		assertFalse(actualExhaust.isPresent()); // Exhaust does not have the autoSave annotation parameter set
	}
}
