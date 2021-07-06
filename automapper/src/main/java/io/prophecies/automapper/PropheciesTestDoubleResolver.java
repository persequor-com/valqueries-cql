package io.prophecies.automapper;

import io.ran.CompoundKey;
import io.ran.KeySet;
import io.ran.MappingHelper;
import io.ran.Property;
import io.ran.RelationDescriber;
import io.ran.TestDoubleDb;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropheciesTestDoubleResolver implements PropheciesResolver {
	private MappingHelper mappingHelper;
	private TestDoubleDb store;

	@Inject
	public PropheciesTestDoubleResolver(MappingHelper mappingHelper, TestDoubleDb store) {
		this.mappingHelper = mappingHelper;
		this.store = store;
	}

	private <FROM, T> Stream<T> getStream(RelationDescriber relationDescriber, FROM from) {
		CompoundKey fromKey = getCompoundKey(from, relationDescriber.getFromKeys());

		return (Stream<T>)store.store.get(relationDescriber.getToClass()).values().stream().filter(o -> {
			T t = (T)o;
			CompoundKey toKey = getCompoundKey(t, relationDescriber.getToKeys());
			return valueEquals(fromKey, toKey);
		});
	}

	private boolean valueEquals(CompoundKey fromKey, CompoundKey toKey) {
		for(int i=0;i<fromKey.getValues().size();i++) {
			if (!(Objects.equals(((Property.PropertyValueList<?>)fromKey.getValues()).get(i).getValue(), ((Property.PropertyValueList<?>)toKey.getValues()).get(i).getValue()))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public <FROM, T> T get(RelationDescriber relationDescriber, FROM from) {
		return (T)getStream(relationDescriber, from).findFirst().orElse(null);
	}

	@Override
	public <FROM, TO> Collection<TO> getCollection(RelationDescriber relationDescriber, FROM from) {
		return (Collection<TO>) getStream(relationDescriber, from).collect(Collectors.toList());

	}

	private <T> CompoundKey getCompoundKey(T t, KeySet toKeys) {
		CompoundKey result = new CompoundKey();
		toKeys.forEach(f -> {
			result.add(((Property)f.getProperty()).value(mappingHelper.getValue(t, f.getProperty())));
		});
		return result;
	}

}
