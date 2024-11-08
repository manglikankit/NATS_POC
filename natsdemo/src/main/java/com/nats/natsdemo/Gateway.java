package com.nats.natsdemo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Gateway {
    private String gatewayId;
    private Location location;
    private String manufacturer;
    private String model;
    private String serialNumber;
    private String firmwareVersion;
    private String installationDate;
    private List<Software> installedSoftware;
    private Interfaces interfaces;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class Location {
    private double latitude;
    private double longitude;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class Software {
    private String name;
    private String version;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class Interfaces {
    private List<Lan> lan;
    private List<Wan> wan;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class Lan {
    private String name;
    private String address;
    private String subnetMask;
    private String gateway;
    private List<String> dnsServers;
    private Boolean enabled;         // For DHCP
    private String leaseDuration;    // For DHCP
    private String server;           // For DHCP
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class Wan {
    private String name;
    private String address;
    private String subnetMask;
    private String gateway;
    private List<String> dnsServers;
    private Boolean enabled;         // For DHCP
    private String leaseDuration;    // For DHCP
    private String server;           // For DHCP
}
