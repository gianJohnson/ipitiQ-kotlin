import kotlinx.coroutines.*
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.fixedRateTimer

interface ProviderHolder {
    fun nextProvider(): Provider
    fun checkProviders()
    fun excludeProviderById(id: String)
    fun includeProviderById(id: String)
    fun incrementCapacity(id: String)
}

abstract class AbstractProviderHolder(hosts: List<Provider>) : ProviderHolder {
    protected val hosts: MutableList<Provider>
    private val excludedProviders: MutableMap<Provider, Int> = mutableMapOf()

    private fun isValidArrayOfProviders(hosts: List<Provider>): Boolean {
        //todo: improvements: create different validation fun with specific feedbacks errors message
        if (hosts.isEmpty())
            return false

        if (hosts.size > 10)
            return false

        return true
    }

    override fun checkProviders() {
        fixedRateTimer(
            name = "hearth-bit",
            initialDelay = 100, period = 3000
        ) {
            checkRegisteredProvider(hosts.iterator())
            //improvement
            includeRecoveredProviders(excludedProviders.keys.iterator())

        }
    }

    private fun checkRegisteredProvider(hostIterator: MutableIterator<Provider>) {
        while (hostIterator.hasNext()) {
            val currentProvider = hostIterator.next()
            //host down
            if (!currentProvider.check()) {
                //remove form active host and add to not active ones
                hostIterator.remove()
                excludedProviders[currentProvider] = 0

            }
        }
    }

    private fun includeRecoveredProviders(hostIterator: MutableIterator<Provider>) {
        while (hostIterator.hasNext()) {
            val currentProvider = hostIterator.next()
            if (currentProvider.check()) {
                if ((excludedProviders[currentProvider]!! + 1) == 2) {
                    hosts.add(currentProvider)
                    hostIterator.remove()
                } else {
                    excludedProviders[currentProvider] = 1
                }
            } else
                excludedProviders[currentProvider] = 0
        }
    }

    override fun incrementCapacity(id: String) {
        hosts.find { it.get() == id }?.incrementCapacity()
    }

    override fun excludeProviderById(id: String) {
        val host: Provider? = hosts.find { it.get() == id }
        if (host != null) {
            excludedProviders[host] = 0
            hosts.remove(host)
        } else
            throw Exception("host not found")

    }

    override fun includeProviderById(id: String) {

        val excludedProvider: Provider? = excludedProviders.keys.find { it.get() == id }
        if (excludedProvider != null) {
            excludedProviders.remove(excludedProvider)
            hosts.add(excludedProvider)
        } else
            throw Exception("host not found")

    }

    protected fun isRequestAcceptable(): Boolean {
        while (hosts.iterator().hasNext()) {
            if (hosts.iterator().next().getCapacity().get() > 0) {
                return true;
            }
        }
        return false;
    }
    init {
        if (this.isValidArrayOfProviders(hosts)) {
            this.hosts = hosts.toMutableList()
        } else
            throw IllegalArgumentException("Given list of host not valid")
    }

}
class RoundRobin(hosts: List<Provider>) : AbstractProviderHolder(hosts) {
    private var nextProviderIndex: Int = 0

    override fun nextProvider(): Provider {
        if (super.isRequestAcceptable()) {
            try {
                val host = this.hosts[this.nextProviderIndex]
                this.nextProviderIndex++
                this.nextProviderIndex %= this.hosts.size
                return host
            } catch (ex: Exception) {
                throw java.lang.Exception("no hosts active. Please try later")
            }
        } else
            throw java.lang.Exception("hosts busy. Please try later")

    }
}
class RandomProvider(hosts: List<Provider>) : AbstractProviderHolder(hosts) {
    override fun nextProvider(): Provider {
        if (super.isRequestAcceptable()) {
            val random = Random()
            if (hosts.size > 0)
                return hosts[random.nextInt(hosts.size)]
            else
                throw java.lang.Exception("none of the registered host is active. Please try later")
        } else
            throw java.lang.Exception("registered hosts are all busy. Please try later")

    }
}

class LoadBalancer(private val providers: ProviderHolder) {
    suspend fun get(): String {
        val deferredValue = coroutineScope {
            async {
                getFromNextProvider()
            }
        }
        val value = deferredValue.await()
        this.providers.incrementCapacity(value)
        return value
    }


    private fun getFromNextProvider(): String {
        val nextProvider = this.providers.nextProvider()
        nextProvider.decrementCapacity()
        //forward the empty request to get provider id
        return nextProvider.get()

    }
    //hearth-bit
    fun checkProvidersScheduler() {
        this.providers.checkProviders()
    }
    //manual exclusion
    fun excludeProviderById(id: String) {
        this.providers.excludeProviderById(id)
    }

    fun includeProviderById(id: String) {
        this.providers.includeProviderById(id)
    }

}

class Provider(private var capacity: AtomicInteger) {
    private val identifier: String = UUID.randomUUID().toString()

    //return the instance id
    fun get(): String {
        return this.identifier
    }
    fun check(): Boolean {
        //to simulate a real scenario
        val random = Random()
        return random.nextBoolean()
    }
    fun getCapacity(): AtomicInteger {
        return this.capacity
    }
    fun decrementCapacity() {
        this.capacity.decrementAndGet()
    }
    fun incrementCapacity() {
        this.capacity.incrementAndGet()
    }
}


fun main(args: Array<String>) {
    //create few hosts (if number is > 10 an exception will be thrown by setting the hosts
    val hosts: List<Provider> = listOf(
        Provider(AtomicInteger(2)),
        Provider(AtomicInteger(4)),
        Provider(AtomicInteger(3)),
        Provider(AtomicInteger(2)),
        Provider(AtomicInteger(1)),
        /*Provider(AtomicInteger(1)),
        Provider(AtomicInteger(1)),
        Provider(AtomicInteger(1)),
        Provider(AtomicInteger(1)),
        Provider(AtomicInteger(1)),
        Provider(AtomicInteger(1))*/


    )
    //print ids for debug
    val numbersIterator = hosts.iterator()
    println("uuids for active registered hosts")
    while (numbersIterator.hasNext())
        println(numbersIterator.next().get());

    //test random
    //test(hosts, LoadBalancer(RandomProvider(hosts)))
    //test round robin
    test(hosts, LoadBalancer(RoundRobin(hosts)))

}

fun test(hosts: List<Provider>, loadBalancer: LoadBalancer) {

    //activate hearth-bit (can make debug quite fun :))
    loadBalancer.checkProvidersScheduler()
    //exclude first host manually
    loadBalancer.excludeProviderById(hosts[0].get())
    //activate scan for excluded host
    while (true) {
        try {
            runBlocking {
                launch {
                    println("try to retrieve host: ")
                    println(loadBalancer.get())
                }
            }
        } catch (e: Exception) {
            println(e.message)
        }
        Thread.sleep(2000)
        try {
            runBlocking {
                launch {
                    println("try to retrieve host: ")
                    println(loadBalancer.get())
                }
            }
        } catch (e: Exception) {
            println(e.message)
        }
        Thread.sleep(2000)
        try {
            runBlocking {
                launch {
                    println("try to retrieve host: ")
                    println(loadBalancer.get())
                }
            }
        } catch (e: Exception) {
            println(e.message)
        }
        Thread.sleep(2000)

    }

}

