@Grapes([
    @Grab(group='com.opencsv', module='opencsv', version='3.9'),
    @Grab(group='com.github.groovy-wslite', module='groovy-wslite', version='1.1.3'),
    @GrabExclude("org.codehaus.groovy:groovy-all:1.7.6")
])

import com.opencsv.CSVReaderBuilder
import groovy.json.JsonBuilder
import wslite.http.auth.HTTPBasicAuthorization
import wslite.rest.ContentType
import wslite.rest.RESTClient

//LastName,FirstName,CarID,CurrentEmploymentType,CurrentEmploymentTitle
cars = [:]
carReader = new CSVReaderBuilder(new FileReader("../car-assignments.csv")).withSkipLines(1).build()
carReader.readAll().each {
    cars << [(it[2]): it]
}

//Timestamp,id,lat,long
bulkPost = new StringBuilder()
gpsReader = new CSVReaderBuilder(new FileReader("../gps.csv")).withSkipLines(1).build()
gpsReader.readAll().each {
    bulkPost.append(new JsonBuilder([index: [] ]).toString()).append("\n")
    bulkPost.append(new JsonBuilder([
        timestamp: it[0],
        id: it[1],
        location: [
            lat: it[2],
            lon: it[3],
        ],
        last_name: cars[it[1]]?.getAt(0) ?: "na",
        first_name: cars[it[1]]?.getAt(1) ?: "na",
        employment_type: cars[it[1]]?.getAt(2) ?: "na",
        employment_title: cars[it[1]]?.getAt(3) ?: "na",
    ]).toString()).append("\n")
}
//println bulkPost.toString()

client = new RESTClient("http://localhost:9200")
client.authorization = new HTTPBasicAuthorization("elastic", "changeme")

client.delete(path: "/vast2014")
client.put(path: "/vast2014") {
    json mappings: [
        record: [
            properties: [
                timestamp       : [type: "date", format: "dd/MM/yyyy HH:mm:ss"],
                id              : [type: "long"],
                location        : [type: "geo_point"],
                last_name       : [type: "keyword"],
                first_name      : [type: "keyword"],
                employment_type : [type: "keyword"],
                employment_title: [type: "keyword"]
            ]
        ]
    ]
}

new File("../bulk.json").withWriter {
    it << bulkPost.toString()
}

//client.post(path: "/vast2014/record/_bulk", headers: ["Content-Type": "application/x-ndjson"]) {
//    text bulkPost.toString()
//}