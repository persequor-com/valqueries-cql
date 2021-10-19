package io.prophecies.automapper;

import io.ran.Mapping;
import io.ran.RelationDescriber;
import io.ran.TypeDescriberImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public class ProphesiesCrudRepositoryImpl<T, K> implements ProphesiesCrudRepository<T, K> {
	private PropheciesBaseCrudRepository<T, K> baseRepo;
	private Class<T> modelType;
	private Class<K> keyType;

	public ProphesiesCrudRepositoryImpl(PropheciesBaseCrudRepository<T, K> baseRepo) {
		this.baseRepo = baseRepo;

		this.modelType = baseRepo.getModelType();
		this.keyType = baseRepo.getKeyType();
	}

	@Override
	public Optional<T> get(K k) {
		return baseRepo.get(k);
	}

	@Override
	public Stream<T> getAll() {
		return baseRepo.getAll();
	}

	@Override
	public CrudUpdateResult deleteById(K k) {
		return baseRepo.deleteById(k);
	}

	@Override
	public CrudUpdateResult save(T t) {
		ChangeMonitor changed = new ChangeMonitor();
		try (PropheciesBatch batch = baseRepo.getBatch()) {
			saveIncludingRelationInternal(changed, batch, t, modelType);
		} catch (IOException exception) {
			throw new RuntimeException(exception);
		}
		return changed::getNumberOfChangedRows;
	}

	private <O, OK> void saveIncludingRelationInternal(ChangeMonitor changed, PropheciesBatch batch, O t, Class<O> modelType) {
		if (changed.isAlreadySaved(t)) {
			return;
		}
		baseRepo.save(batch, t, modelType);
		TypeDescriberImpl.getTypeDescriber(modelType).relations().forEach(relationDescriber -> {
			internalSaveRelation(changed, batch, t, relationDescriber);
		});
	}


	private void internalSaveRelation(ChangeMonitor changed, PropheciesBatch tx, Object t, RelationDescriber relationDescriber) {
		if (!relationDescriber.getRelationAnnotation().autoSave()) {
			return;
		}
		if (!(t instanceof Mapping)) {
			throw new RuntimeException("Valqueries models should have a @Mapper annotation");
		}
		Mapping mapping = (Mapping)t;
		Object relation = mapping._getRelation(relationDescriber);
		if (relation != null) {
			if (relationDescriber.isCollectionRelation()) {
				Collection<Object> relations = (Collection<Object>) relation;
				for (Object r : relations) {
					saveIncludingRelationInternal(changed, tx, r, (Class<Object>) relationDescriber.getToClass().clazz);
				}

			} else {
				saveIncludingRelationInternal(changed, tx, relation, (Class<Object>) relationDescriber.getToClass().clazz);
			}
		}
	}

	protected  PropheciesQuery<T> query() {
		return baseRepo.query();
	}
}
