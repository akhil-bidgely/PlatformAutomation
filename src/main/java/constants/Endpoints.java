package constants;

public class Endpoints {
    public static final  String GET_USER="v2.0/users/{uuid}";
    public static final  String GET_PILOT_CONFIGS="/entities/pilot/{pilotId}/configs/launchpad_ingestion_configs";
    public static final  String GET_METERS="/meta/users/{uuid}/homes/1/gws/{gws}/meters";
    public static final  String GB_JSON="/streams/users/{uuid}/homes/1/gws/{gws}/meters/1/gb.json";
    public static final  String LABEL_TIMESTAMP="/streams/users/{uuid}/homes/1/labelTimestamps/ELECTRIC/Default";
    public static final  String PARTNER_USERID="/v2.0/users/";
    public static final  String GENERATE_TOKEN="/oauth/token?grant_type=client_credentials&scope=all";
    public static final  String GB_DISAGG="/2.1/gb-disagg/process-request/{uuid}/{hid}?start={t0}&end={t1}";
    public static final  String UTILITY_DATA="/billingdata/users/{uuid}/homes/1/utilitydata";
    public static final  String USER_CONFIG="/entities/user/{uuid}/configs";
}
