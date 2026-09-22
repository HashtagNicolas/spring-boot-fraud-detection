package com.hashtag.ngo.example.fraud.cucumber;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;

/**
 * Point d'entrée JUnit 5 pour exécuter les scénarios Cucumber via Maven
 * Surefire, au même titre que les autres tests du module.
 *
 * Le nom de cette classe doit se terminer par "Test" : les motifs
 * d'inclusion par défaut de maven-surefire-plugin ne retiennent que les
 * classes dont le nom commence ou finit par "Test" (ou finit par
 * "TestCase") ; une classe nommée "...TestSuite" ne serait donc jamais
 * exécutée.
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.hashtag.ngo.example.fraud.cucumber")
public class RunCucumberTest {
}
