package org.example;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultMessage;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

//https://camel.apache.org/components/latest/

public class Starter {
    public static void main(String[] args) throws Exception {
        CamelContext camel = new DefaultCamelContext();

        camel.getPropertiesComponent().setLocation("classpath:application.properties");

        DataSource dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://localhost:5432/catalizator?user=postgres&password=2212"
        );
        camel.getRegistry().bind("catalizator", dataSource);

        ProducerTemplate producerTemplate = camel.createProducerTemplate();

        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file:///home/riezenmark/Рабочий стол/Projects/Java/camel-app/files/from")
                        .routeId("File processing")
//                        .log(">>>> ${body}")
                        .convertBodyTo(String.class)
                        .to("log:?showBody=true&showHeaders=true")
                        .choice()
                        .when(exchange -> exchange.getIn().getBody(String.class).contains("=a"))
                            .to("file:///home/riezenmark/Рабочий стол/Projects/Java/camel-app/files/toA")
                        .when(exchange -> exchange.getIn().getBody(String.class).contains("=b"))
                            .to("file:///home/riezenmark/Рабочий стол/Projects/Java/camel-app/files/toB")
                        .otherwise()
                            .to("file:///home/riezenmark/Рабочий стол/Projects/Java/camel-app/files/default");
            }
        });

        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:base?period=60000")
                        .routeId("JDBC route")
                        .setHeader("key", constant(1))
                        .setBody(simple("select id, data from message where id > :?key"))
                        .to("jdbc:catalizator?useHeadersAsParameters=true")
                        .log(">>>> ${body}")
                        .process(exchange -> {
                            Message in = exchange.getIn();
                            String body = in.getBody(String.class);

                            DefaultMessage message = new DefaultMessage(exchange);
                            message.setHeaders(in.getHeaders());
                            message.setHeader("custom_header", "qwerty");
                            message.setBody(String.format("%s\n%s", body, in.getHeaders().toString()));

                            exchange.setMessage(message);
                        })
                        .toD("file:/home/riezenmark/Рабочий стол/Projects/Java/camel-app/files/database?fileName=done-${date:now:yyyyMMdd}-${headers.custom_header}.txt");
            }
        });

        camel.start();

        producerTemplate.sendBody(
                "file:/home/riezenmark/Рабочий стол/Projects/Java/camel-app/files?fileName=event-${date:now:yyyyMMdd-HH-mm}.html",
                "<hello>world!</hello>"
        );

        Thread.sleep(4_000);
        camel.stop();
    }
}
