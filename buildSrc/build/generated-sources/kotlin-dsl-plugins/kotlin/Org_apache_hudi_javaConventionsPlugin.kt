/**
 * Precompiled [org.apache.hudi.java-conventions.gradle.kts][Org_apache_hudi_java_conventions_gradle] script plugin.
 *
 * @see Org_apache_hudi_java_conventions_gradle
 */
class Org_apache_hudi_javaConventionsPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Org_apache_hudi_java_conventions_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
