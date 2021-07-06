package io.prophecies.automapper;

import io.ran.Mapper;
import io.ran.Relation;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Mapper(dbType = Prophecies.class)
public class Car {
	private UUID id;
	private String title;
	private Brand brand;
	private ZonedDateTime createdAt;
	private UUID exhaustId;
	private List<String> numbers = new ArrayList<>();
	@Relation(collectionElementType = Door.class)
	private transient List<Door> doors;
	@Relation
	private transient Exhaust exhaust;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public ZonedDateTime getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(ZonedDateTime createdAt) {
		this.createdAt = createdAt;
	}

	public List<Door> getDoors() {
		return doors;
	}

	public void setDoors(List<Door> doors) {
		this.doors = doors;
	}

	public Brand getBrand() {
		return brand;
	}

	public void setBrand(Brand brand) {
		this.brand = brand;
	}

	public List<String> getNumbers() {
		return numbers;
	}

	public void setNumbers(List<String> numbers) {
		this.numbers = numbers;
	}

	public Exhaust getExhaust() {
		return exhaust;
	}

	public void setExhaust(Exhaust exhaust) {
		this.exhaust = exhaust;
		this.exhaustId = exhaust.getId();
	}

	public UUID getExhaustId() {
		return exhaustId;
	}

	public void setExhaustId(UUID exhaustId) {
		this.exhaustId = exhaustId;
	}
}
