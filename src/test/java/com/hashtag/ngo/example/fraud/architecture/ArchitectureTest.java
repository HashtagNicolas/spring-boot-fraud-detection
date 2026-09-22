package com.hashtag.ngo.example.fraud.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

/**
 * Règles d'architecture vérifiées sur le code de production uniquement (le
 * code de test est exclu via DoNotIncludeTests). Les classes générées ou
 * synthétiques (classes internes/anonymes compilées séparément, du type
 * Foo$1.class) sont également exclues via ExcludeGeneratedAndSyntheticClasses :
 * ni la convention de nommage (*Impl) ni le découpage par paquetage ne leur
 * sont applicables.
 */
@AnalyzeClasses(
        packages = "com.hashtag.ngo.example.fraud",
        importOptions = {
                ImportOption.DoNotIncludeTests.class,
                ArchitectureTest.ExcludeGeneratedAndSyntheticClasses.class
        })
class ArchitectureTest {

    public static class ExcludeGeneratedAndSyntheticClasses implements ImportOption {
        @Override
        public boolean includes(Location location) {
            // Exclut les .class dont le nom de fichier contient "$" : classes
            // anonymes, locales ou membres compilées séparément (Foo$1.class,
            // Foo$1$2.class...). Les lambdas, elles, ne produisent aucun
            // fichier .class séparé (invokedynamic) et n'ont donc pas besoin
            // d'être filtrées ici.
            return !location.contains("$");
        }
    }

    @ArchTest
    static final ArchRule interfaces_do_not_reside_in_an_impl_package =
            classes()
                    .that().areInterfaces()
                    .should().resideOutsideOfPackage("..impl..")
                    .as("les interfaces ne doivent pas résider dans un package .impl");

    @ArchTest
    static final ArchRule implementations_reside_in_an_impl_package =
            classes()
                    .that().haveSimpleNameEndingWith("Impl")
                    .should().resideInAPackage("..impl..")
                    .as("les classes *Impl doivent résider dans un package .impl");

    // Couches de l'application et dépendances autorisées entre elles, telles
    // qu'observées dans le code : api (contrôleurs REST, DTO) ; bean
    // (interfaces de service + implémentations *.impl) ; entity (modèle de
    // domaine pur, sans dépendance vers les autres couches) ; repository
    // (accès Spring Data JPA) ; listener (adaptateurs Kafka entrants) ;
    // config (câblage Spring/Kafka) ; security (filtre JWT + configuration
    // Spring Security, s'appuie sur bean.JwtService).
    @ArchTest
    static final ArchRule layers_are_respected =
            layeredArchitecture()
                    .consideringAllDependencies()
                    .layer("Api").definedBy("..api..")
                    .layer("Bean").definedBy("..bean..")
                    .layer("Entity").definedBy("..entity..")
                    .layer("Repository").definedBy("..repository..")
                    .layer("Listener").definedBy("..listener..")
                    .layer("Config").definedBy("..config..")
                    .layer("Security").definedBy("..security..")

                    // api : point d'entrée, n'est jamais dépendu par les autres couches.
                    .whereLayer("Api").mayNotBeAccessedByAnyLayer()
                    // bean (interfaces + impl) : utilisé par l'API, les listeners Kafka
                    // et le filtre/la configuration de sécurité (JwtService).
                    .whereLayer("Bean").mayOnlyBeAccessedByLayers("Api", "Listener", "Security")
                    // entity : modèle de domaine, utilisé par toutes les couches
                    // applicatives sauf config et security (câblage technique uniquement).
                    .whereLayer("Entity").mayOnlyBeAccessedByLayers("Api", "Bean", "Repository", "Listener")
                    // repository : accès aux données, réservé à la couche bean (impl).
                    .whereLayer("Repository").mayOnlyBeAccessedByLayers("Bean")
                    // listener : adaptateur Kafka entrant, jamais dépendu ailleurs.
                    .whereLayer("Listener").mayNotBeAccessedByAnyLayer()
                    // config : câblage Spring/Kafka, consommé par bean (impl) et listener.
                    .whereLayer("Config").mayOnlyBeAccessedByLayers("Bean", "Listener")
                    // security : câblage Spring Security, jamais dépendu ailleurs
                    // (les filtres/configurations sont détectés par Spring, pas importés).
                    .whereLayer("Security").mayNotBeAccessedByAnyLayer()

                    .as("les couches api/bean/entity/repository/listener/config/security ne doivent être accédées que par les couches autorisées");
}
