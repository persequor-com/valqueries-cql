package io.prophecies.automapper;

public class TestDoubleGuiceModule extends GuiceModule {
	public TestDoubleGuiceModule() {
		super(null, PropheciesTestDoubleResolver.class);
	}

	@Override
	protected void configure() {
		super.configure();
		bind(PropheciesRepositoryFactory.class).to(PropheciesTestDoubleRepositoryFactory.class);
	}
}
