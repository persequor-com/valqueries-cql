package io.prophecies.automapper;

import io.ran.CompoundKey;
import io.ran.GenericFactory;
import io.ran.KeySet;
import io.ran.MappingHelper;
import io.ran.Property;
import io.ran.RelationDescriber;
import io.ran.TypeDescriber;
import io.ran.TypeDescriberImpl;
import io.prophecies.Cassandra;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropheciesResolverImpl implements PropheciesResolver {
	private final GenericFactory genericFactory;
	private CqlGenerator cqlGenerator;
	private Cassandra cassandra;
	private MappingHelper mappingHelper;

	@Inject
	public PropheciesResolverImpl(GenericFactory genericFactory, CqlGenerator cqlGenerator, Cassandra cassandra, MappingHelper mappingHelper) {
		this.genericFactory = genericFactory;
		this.cqlGenerator = cqlGenerator;
		this.cassandra = cassandra;
		this.mappingHelper = mappingHelper;
	}

	private <FROM, T> Stream<T> getStream(RelationDescriber relationDescriber, FROM from) {

		TypeDescriber fromTypeDescriber = (TypeDescriber) TypeDescriberImpl.getTypeDescriber(relationDescriber.getToClass().clazz);

		CompoundKey fromKey = getRelationKey(fromTypeDescriber, relationDescriber.getFromKeys(), from);
		KeySet toKey = relationDescriber.getToKeys();
		PropheciesQueryImpl<?> query = new PropheciesQueryImpl(cassandra, relationDescriber.getToClass().clazz, cqlGenerator, genericFactory, mappingHelper);
		int i = 0;
		for (Property.PropertyValue<?> k : ((Property.PropertyValueList<?>) fromKey.getValues())) {
			Property to =toKey.toProperties().get(i);
			query = (PropheciesQueryImpl)query.eq(to.value(k.getValue()));
			i++;
		}
		return (Stream<T>) query.execute();
	}

	private CompoundKey getRelationKey(TypeDescriber typeDescriber, KeySet fromKeys, Object t) {
		CompoundKey compoundKey = new CompoundKey();
		fromKeys.forEach(f -> {
			compoundKey.add(((Property)f.getProperty()).value(mappingHelper.getValue(t, f.getProperty())));
		});
		return compoundKey;
	}

	private boolean valueEquals(CompoundKey fromKey, CompoundKey toKey) {
		for(int i=0;i<fromKey.getValues().size();i++) {
			if (!(Objects.equals(((Property.PropertyValueList<?>)fromKey.getValues()).get(i).getValue(), ((Property.PropertyValueList<?>)toKey.getValues()).get(i).getValue()))) {
				return false;
			}
		}
		return true;
	}


	private <T> CompoundKey getCompoundKey(T t, KeySet toKeys) {
		CompoundKey result = new CompoundKey();
		toKeys.forEach(f -> {
			result.add(((Property)f.getProperty()).value(mappingHelper.getValue(t, f.getProperty())));
		});
		return result;
	}

	@Override
	public <FROM, T> Collection<T> getCollection(RelationDescriber relationDescriber, FROM from) {
		return (Collection<T>) getStream(relationDescriber, from).collect(Collectors.toList());
	}


	@Override
	public <FROM, TO> TO get(RelationDescriber relationDescriber, FROM from) {
		Optional<TO> o = (Optional<TO>) getStream(relationDescriber, from).findFirst();
		return o.orElse(null);
	}

}
