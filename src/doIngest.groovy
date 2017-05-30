@Grapes([
    @Grab(group='com.opencsv', module='opencsv', version='3.9'),
    @Grab(group='com.github.groovy-wslite', module='groovy-wslite', version='1.1.3'),
    @GrabExclude("org.codehaus.groovy:groovy-all:1.7.6")
])

import com.opencsv.CSVReaderBuilder
import wslite.http.auth.HTTPBasicAuthorization
import wslite.rest.ContentType
import wslite.rest.RESTClient

client = new RESTClient("http://localhost:9200")
client.authorization = new HTTPBasicAuthorization("elastic", "changeme")
client.setDefaultAcceptHeader(ContentType.JSON)

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

//LastName,FirstName,CarID,CurrentEmploymentType,CurrentEmploymentTitle
cars = [:]
carReader = new CSVReaderBuilder(new FileReader("../car-assignments.csv")).withSkipLines(1).build()
carReader.readAll().each {
    cars << [(it[2]): it]
}

//Timestamp,id,lat,long
gpsReader = new CSVReaderBuilder(new FileReader("../gps.csv")).withSkipLines(1).build()
gpsReader.readAll().each { record ->
    client.post(path: "/vast2014/record") {
        json timestamp: record[0],
            id: record[1],
            location: [
                lat: record[2],
                lon: record[3],
            ],
            last_name: cars[record[1]]?.getAt(0) ?: "na",
            first_name: cars[record[1]]?.getAt(1) ?: "na",
            employment_type: cars[record[1]]?.getAt(2) ?: "na",
            employment_title: cars[record[1]]?.getAt(3) ?: "na"
    }
    println "posted record: [$record]"
}