package it.unipol.alfa.cpm.portal.report.batch.service;

import java.sql.SQLException;
import it.unipol.alfa.cpm.portal.report.batch.config.ConfigExternalLoader;
import it.unipol.alfa.cpm.portal.report.batch.constants.ParamConstants;
import it.unipol.alfa.cpm.portal.report.batch.constants.UtilityConstants;
import it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import it.unipol.alfa.cpm.portal.report.batch.util.ConnectionHandler;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import javax.annotation.PostConstruct;
import static it.unipol.alfa.cpm.portal.report.batch.constants.UtilityConstants.NEW_LINE;

@Slf4j
@Service
public class LoadRequestBdhHandlerService {

    @Autowired
    private UtilityHandler utilityHandler;

    @Autowired
    private ConnectionHandler connectionHandler;

    @Autowired
    private StoreProcedureExecutionHandler storeProcExecutionHandler;

    @Autowired
    private ConfigExternalLoader configExternalLoader;

    @Autowired
    UtilityFileHandler utilityFileHandler;

    private static final String CLEAR_LOG = "Try clear {} query: {}";

    private static final String STG_SOMMARIO_CRASH_ALFA = "STG_SOMMARIO_CRASH_ALFA";
    private static final int STG_SOMMARIO_CRASH_ALFA_COL_NUM = 40;
    // Posizioni
    private static final String STG_POSIZIONI_CRASH_ALFA = "STG_POSIZIONI_CRASH_ALFA";
    private static final int STG_POSIZIONI_CRASH_ALFA_COL_NUM = 13;
    private static final String STG_ACCELERAZIONI_CRASH_ALFA = "STG_ACCELERAZIONI_CRASH_ALFA";
    private static final int STG_ACCELERAZIONI_CRASH_ALFA_COL_NUM = 7;
    private static final String STG_GIROSCOPIO_CRASH_ALFA = "STG_GIROSCOPIO_CRASH_ALFA";
    private static final int STG_GIROSCOPIO_CRASH_ALFA_COL_NUM = 7;
    private static final String STG_CALIBRAZIONE_CRASH_ALFA = "STG_CALIBRAZIONE_CRASH_ALFA";
    private static final int STG_CALIBRAZIONE_CRASH_ALFA_COL_NUM = 9;

    private int chunkSize;

    public static final String STG_SEZIONE_GENERALE_ALFA = "STG_SEZIONE_GENERALE_ALFA";
    public static final String STG_VIAGGIO_SOMMARIO_ALFA = "STG_VIAGGIO_SOMMARIO_ALFA";
    private static final int STG_VIAGGIO_SOMMARIO_ALFA_COL_NUM = 26;
    public static final String STG_VIAGGIO_POSIZIONI_ALFA = "STG_VIAGGIO_POSIZIONI_ALFA";
    private static final int STG_VIAGGIO_POSIZIONI_ALFA_COL_NUM = 12;
    public static final String STG_ANOMALIE_ALFA = "STG_ANOMALIE_ALFA";
    public static final String STG_SOSTE_ALFA = "STG_SOSTE_ALFA";
    private static final int STG_SOSTE_ALFA_COL_NUM = 14;
    private static final String STG_STATO_DEVICE_ALFA = "STG_STATO_DEVICE_ALFA";

    private static final String REPLACE_CHARACTERS_REGEX = "[^0-9a-zA-Z\\u4e00-\\u9fa5.?,????]+";

    private InputBdhRequest bdhRequest;

    @PostConstruct
    public void init(){
        String chunkProperty = configExternalLoader.getProperty(ParamConstants.CHUNK_SIZE);
        if (chunkProperty != null){
            chunkSize = Integer.parseInt(chunkProperty);
        } else {
            chunkSize = 100;
        }
    }

    public void setBdhRequest(InputBdhRequest bdhRequestParam){
        bdhRequest = bdhRequestParam;
    }

    public void operationalMainLoadIntoDB() throws SQLException {
        log.debug("Begin load request BDH into tabels for: " + bdhRequest.getClaimId());
        loadBdhRequestIntoDB();
        subagenziaRichiesta();
        tagRichiesta();

        insertCrashesIntoStagingTables();
        log.debug("END load request BDH into tabels: " + bdhRequest.getClaimId());

    }

    public void insertCrashesIntoStagingTables() throws SQLException {
        log.trace("Begin create files for request : "+ bdhRequest.getClaimId());

        insertCrashesHeader(bdhRequest.getCrashes(),bdhRequest.getClaimId());
        insertCalibrazioniCrash(bdhRequest.getCrashes(),bdhRequest.getClaimId());
        insertCrashPositions(bdhRequest.getCrashes(), bdhRequest.getClaimId());
        log.debug("End create files for request : "+bdhRequest.getClaimId());
    }

    private void subagenziaRichiesta() {
        log.debug("BEGIN update subagenzia");
        connectionHandler.updateSubagenziaRichiesta(bdhRequest.getClaimId(),bdhRequest.getSubagenzia());
        log.debug("END update subagenzia");
    }
    
    private void tagRichiesta() {
        log.debug("BEGIN update tag");
        connectionHandler.updateTagRichiesta(bdhRequest.getClaimId(),bdhRequest.getTag());
        log.debug("END update tag");
    }
    
    public void operationalMainExecuteAllStoreProc() throws SQLException {
        log.debug("Begin execution store procedure for all request ");
        storeProcExecutionHandler.executeAllTheStoreDetail();
        log.info("END Store procedure executed for all request  ");
    }

    public void insertCrashesHeader(List<Crash> crashList, String idClaim) throws SQLException {
        log.trace("Begin inserting crash heading for claim : "+idClaim+" num : "+crashList.size());
        StringBuilder bufferToAdd = new StringBuilder();
        int currentCount = 0;
        for (Crash crashRtRequest : crashList) {

            String dataCrashFormatted = crashRtRequest.getIstanteCrash();
            String dataRegistrazioneFormatted = crashRtRequest.getDataRegistrazione();

            String indirizzo = crashRtRequest.getIndirizzo() != null ? crashRtRequest.getIndirizzo().replaceAll(REPLACE_CHARACTERS_REGEX," "): "";
            String comune = crashRtRequest.getComune() != null ? crashRtRequest.getComune().replaceAll(REPLACE_CHARACTERS_REGEX," ") : "";
            String categoriaStrada = crashRtRequest.getCategoriaStrada() != null ? crashRtRequest.getCategoriaStrada().replaceAll(REPLACE_CHARACTERS_REGEX," ") : "";
            String ragioneSocialeInstallatore = crashRtRequest.getRagioneSocialeInstallatore() != null ? crashRtRequest.getRagioneSocialeInstallatore().replaceAll(REPLACE_CHARACTERS_REGEX," ") : "";

            SensoDiMarcia sensoDiMarcia = crashRtRequest.getSensoDiMarcia() == null ? new SensoDiMarcia() : crashRtRequest.getSensoDiMarcia();

            String tipologiaCrash = crashRtRequest.getTipologiaCrash();
            String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(idClaim, crashRtRequest.getTipoRiga(), crashRtRequest.getIdCrash(), dataCrashFormatted, crashRtRequest
                            .getAccelerazione(), crashRtRequest.getProvincia(), indirizzo, crashRtRequest.getCap(), comune, categoriaStrada,
                    crashRtRequest.getCodiceCompagnia(), ragioneSocialeInstallatore, crashRtRequest.getCodiceInstallatore(), crashRtRequest
                            .getLatitudine(), crashRtRequest.getLongitudine(), crashRtRequest.getModulo(), dataRegistrazioneFormatted, tipologiaCrash, crashRtRequest
                            .getAlgoritmoRilevazione(), crashRtRequest.getCrashRicevuto(), crashRtRequest.getCrashAttivazione(), crashRtRequest.getQualitaGps(), crashRtRequest
                            .getCrashFiltrato(), crashRtRequest.getCrashAssistenza(), crashRtRequest.getAccPrimoUrto(), crashRtRequest.getAngoloPrimoUrto(),
                    crashRtRequest.getAccSecondoUrto(), crashRtRequest.getAngoloSecondoUrto(), crashRtRequest.getTempoSecondoUrto(), crashRtRequest.getAccTerzoUrto(), crashRtRequest
                            .getAngoloTerzoUrto(), crashRtRequest.getTempoTerzoUrto(), crashRtRequest.getDeviceAttivo(), crashRtRequest.getIdAssistenza(),
                    sensoDiMarcia.getStato(), sensoDiMarcia.getCompletato(),crashRtRequest.getCalibratedDevice(), crashRtRequest.getDriverUsername(), crashRtRequest.getDriverTaxId(),
                    crashRtRequest.getFlagRiassegnato() != null ? getIntFromStringBoolean(crashRtRequest.getFlagRiassegnato()) : null);

            bufferToAdd.append(lineToAdd)
                    .append(NEW_LINE);
            currentCount++;

            if (currentCount % chunkSize == 0) {
                connectionHandler.insertBufferToDb(bufferToAdd, STG_SOMMARIO_CRASH_ALFA, STG_SOMMARIO_CRASH_ALFA_COL_NUM, chunkSize);
                bufferToAdd = new StringBuilder(); // svuota il buffer e libera memoria
            }
        }
        connectionHandler.insertBufferToDb(bufferToAdd, STG_SOMMARIO_CRASH_ALFA, STG_SOMMARIO_CRASH_ALFA_COL_NUM, chunkSize);
        log.trace("End create crashies Files for claim : "+idClaim+" num : "+crashList.size());
    }

    public void insertCalibrazioniCrash(List<Crash> crashies, String idClaim) throws SQLException {
        StringBuilder bufferCalibrazioneToAdd = new StringBuilder();
        int currentCount = 0;
        for (Crash crashTemp : crashies) {
            MatriceRotazione matriceRotazione = crashTemp.getMatriceRotazione();
            String idCrash = crashTemp.getIdCrash();
            String valuesSql = utilityFileHandler.sqlGenerateValuesStringForFileCsv(matriceRotazione.getTipoRiga(), matriceRotazione.getAlfaX(), matriceRotazione.getAlfaY(),
                    matriceRotazione.getAlfaZ(), matriceRotazione.getOffsetX(), matriceRotazione.getOffsetY(), matriceRotazione.getOffsetZ(), idCrash, idClaim);
            bufferCalibrazioneToAdd.append(valuesSql)
                                    .append(NEW_LINE);

            currentCount++;
            if (currentCount % chunkSize == 0) {
                connectionHandler.insertBufferToDb(bufferCalibrazioneToAdd, STG_CALIBRAZIONE_CRASH_ALFA, STG_CALIBRAZIONE_CRASH_ALFA_COL_NUM, chunkSize);
                bufferCalibrazioneToAdd = new StringBuilder();
            }
        }
        connectionHandler.insertBufferToDb(bufferCalibrazioneToAdd, STG_CALIBRAZIONE_CRASH_ALFA, STG_CALIBRAZIONE_CRASH_ALFA_COL_NUM, chunkSize);
    }

    public void insertCrashPositions(List<Crash> crashies, String idClaim) throws SQLException {
        log.trace("Begin create posizioni Files for claim : "+idClaim);
        StringBuilder bufferPosizioniToAdd = new StringBuilder();
        StringBuilder bufferPosizioniAccToAdd = new StringBuilder();
        StringBuilder bufferPosizioniGirToAdd = new StringBuilder();
        int crashesCounter = 0;
        int accCounter = 0;
        int girCounter = 0;
        for (Crash crashTemp : crashies) {
            String idCrash = crashTemp.getIdCrash();
            List<Posizioni> posizioniList =  crashTemp.getPosizioni();
            for (Posizioni posTemp : posizioniList) {
                String momentoCrash = posTemp.getPosizioneCrash();
                String numDistInter = org.springframework.util.StringUtils.isEmpty(posTemp.getDistanza()) ? null : posTemp.getDistanza();
                String linePosToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(posTemp.getTipoRiga(), posTemp.getIstante(), posTemp.getLatitudine(), posTemp.getLongitudine(),
                        posTemp.getQualitaGps(), momentoCrash, posTemp.getVelocitaGps(), posTemp.getHeadingGps(), posTemp.getStatoQuadro(), numDistInter, idCrash, idClaim,posTemp.getPositionType());

                bufferPosizioniToAdd.append(linePosToAdd)
                                    .append(NEW_LINE);

                crashesCounter++;
                if (crashesCounter % chunkSize == 0) {
                    connectionHandler.insertBufferToDb(bufferPosizioniToAdd, STG_POSIZIONI_CRASH_ALFA, STG_POSIZIONI_CRASH_ALFA_COL_NUM, chunkSize);
                    bufferPosizioniToAdd = new StringBuilder();
                }


                log.trace("Begin create buffer crash-posizioni-accelerazioni :" + idCrash + " idClaim: " + idClaim);
                for (Accelerazioni accTemp : posTemp.getAccelerazioni()) {
                    String linePosAccToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(accTemp.getTipoRiga(), accTemp.getAccX(), accTemp.getAccY(), accTemp.getAccZ(), posTemp.getIstante(), idCrash,idClaim);

                    bufferPosizioniAccToAdd.append(linePosAccToAdd)
                                            .append(NEW_LINE);
                    accCounter++;
                    if (accCounter % chunkSize == 0) {
                        connectionHandler.insertBufferToDb(bufferPosizioniAccToAdd, STG_ACCELERAZIONI_CRASH_ALFA, STG_ACCELERAZIONI_CRASH_ALFA_COL_NUM, chunkSize);
                        bufferPosizioniAccToAdd = new StringBuilder();
                    }
                }

                log.trace("Begin create buffer crash-posizioni-giroscopi :" + idCrash + " idClaim: " + idClaim);

                for (Giroscopus girTemp : posTemp.getGiroscopi()) {
                    String linePosGirToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(girTemp.getTipoRiga(), girTemp.getGyroX(), girTemp.getGyroY(), girTemp.getGyroZ(), posTemp.getIstante(), idCrash, idClaim);
                    bufferPosizioniGirToAdd.append(linePosGirToAdd)
                                            .append(NEW_LINE);

                    girCounter++;
                    if (girCounter % chunkSize == 0) {
                        connectionHandler.insertBufferToDb(bufferPosizioniGirToAdd, STG_GIROSCOPIO_CRASH_ALFA, STG_GIROSCOPIO_CRASH_ALFA_COL_NUM, chunkSize);
                        bufferPosizioniGirToAdd = new StringBuilder();
                    }
                }
            }
        }
        log.info("Inserting remaining posizioni buffer into {}", STG_POSIZIONI_CRASH_ALFA);
        connectionHandler.insertBufferToDb(bufferPosizioniToAdd, STG_POSIZIONI_CRASH_ALFA, STG_POSIZIONI_CRASH_ALFA_COL_NUM, chunkSize);
        log.info("Inserting remaining posizioni accelerazioni buffer into {}", STG_ACCELERAZIONI_CRASH_ALFA);
        connectionHandler.insertBufferToDb(bufferPosizioniAccToAdd, STG_ACCELERAZIONI_CRASH_ALFA, STG_ACCELERAZIONI_CRASH_ALFA_COL_NUM, chunkSize);
        log.info("Inserting remaining posizioni giroscopi buffer into {}", STG_GIROSCOPIO_CRASH_ALFA);
        connectionHandler.insertBufferToDb(bufferPosizioniGirToAdd, STG_GIROSCOPIO_CRASH_ALFA, STG_GIROSCOPIO_CRASH_ALFA_COL_NUM, chunkSize);
    }
    
    private void loadBdhRequestIntoDB() throws SQLException {
        log.trace("Begin load all the request into DB  for : " + bdhRequest.getClaimId());

        wrapInsertHeader();
        log.trace("Header insert: " + bdhRequest.getClaimId());

        wrapInsertAnomalie(bdhRequest.getClaimId());
        log.trace("Anomalie Inserted: " + bdhRequest.getClaimId());

        wrapInsertSoste(bdhRequest.getClaimId());
        log.trace("Soste Inserted: " + bdhRequest.getClaimId());

        wrapInsertViaggi(bdhRequest.getClaimId());
        log.trace("Viaggi and posizioni Inserted: " + bdhRequest.getClaimId());

        wrapInsertStatiDevice(bdhRequest.getClaimId());
        log.trace("Stati Device Inserted: " + bdhRequest.getClaimId());

        log.debug("END correctly load all the request into DB  for : " + bdhRequest.getClaimId());
    }
    
    public void wrapInsertHeader() throws SQLException {
        String tipoContratto = StringUtils.abbreviate(bdhRequest.getTipoContratto(), 17);
        String versFirmware = StringUtils.abbreviate(bdhRequest.getVersFirmware(), 10);

        String columnsSql = utilityHandler.sqlGenerateColumnForQuery("TIPO_RIGA", "CLAIM_ID", "TARGA", "DATA_ORA", "MODULO", "TIPO_CONTRATTO", "DATA_ULTIMO_DOWNLOAD", "NUM_CRASH",
                "NUM_VIAGGI", "NUM_ANOMALIE", "NUM_NON_ASSOCIAZIONE", "TIPO_DISPOSITIVO", "NUM_FREQ_CAMP_GPS", "NUM_FREQ_CAMP_ACC", "NUM_FREQ_CAMP_GIR", "DESC_VER_FIRMWARE",
                "DESC_TEL1", "DESC_TEL2", "DESC_TEL3", "DESC_TEL4", "PRODUTTORE", "MODELLO");

        String ultimoDownload = null;
        if(bdhRequest.getDataUltimoDownloadDati() != null )
            ultimoDownload =  utilityHandler.normalizeDateForDb(bdhRequest.getDataUltimoDownloadDati(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS, UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);

        String valuesSql = utilityHandler.sqlGenerateValuesStringForQuery(bdhRequest.getTipoRiga(), bdhRequest.getClaimId(), bdhRequest.getTarga(),
                bdhRequest.getIstanteRichiesta(), bdhRequest.getModulo(), tipoContratto, ultimoDownload, bdhRequest.getNumeroCrashTrovati().toString(),
                bdhRequest.getNumeroViaggiTrovati().toString(), bdhRequest.getNumeroAnomalie().toString(), bdhRequest.getNumeroPeriodiMancataAssociazione(),
                (bdhRequest.getTipologiaDispositivo() != null && bdhRequest.getTipologiaDispositivo().length() <= 1 ? bdhRequest.getTipologiaDispositivo() : null), bdhRequest.getFreqGps(), bdhRequest.getFreqAcc(), bdhRequest.getFreqGyro(), versFirmware,
                bdhRequest.getTelefono1(), bdhRequest.getTelefono2(), bdhRequest.getTelefono3(), bdhRequest.getTelefono4(), bdhRequest.getProduttoreBlackBox(),
                bdhRequest.getHWModel());

        String sqlInsert = connectionHandler.createInsertSql(STG_SEZIONE_GENERALE_ALFA, columnsSql, valuesSql);

        try {
            log.trace("Try insert into STG_SEZIONE_GENERALE_ALFA query: " + sqlInsert);
            connectionHandler.executeUpdate(sqlInsert);
        } catch (SQLException e) {
            utilityHandler.handleExceptionLog("", e.getMessage(), e);
            throw new SQLException("Error insert header : " + e.getMessage());
        }
    }
    
    public void wrapInsertAnomalie(String idClaim) throws SQLException {
        List<Anomalie> anomalieList = this.bdhRequest.getAnomalie();
        for (Anomalie anomaliaTemp : anomalieList) {
            String columnSql = utilityHandler.sqlGenerateColumnForQuery("TIPO_RIGA", "TIPO_ANOMALIA", "STATO_ANOMALIA", "DATA_ORA_INIZIO", "DATA_ORA_FINE", "ID_CLAIM");
            String valuesSql = utilityHandler.sqlGenerateValuesStringForQuery(anomaliaTemp.getTipoRiga(), anomaliaTemp.getTipoAnomalia(), anomaliaTemp.getStatoAnomalia(),
                    anomaliaTemp.getIstanteInizio(), anomaliaTemp.getIstanteFine(), idClaim);
            String insertSqlComplete = connectionHandler.createInsertSql(STG_ANOMALIE_ALFA, columnSql, valuesSql);

            try {
                log.trace("Try insert into STG_ANOMALIE_ALFA query: " + insertSqlComplete);
                connectionHandler.executeUpdate(insertSqlComplete);
            } catch (SQLException e) {
                utilityHandler.handleExceptionLog("", e.getMessage(), e);
                throw new SQLException("Error insert crash : " + e.getMessage());
            }
        }
    }

    public void wrapInsertSoste(String idClaim) throws SQLException {
        long startTime = System.nanoTime();  // inizio misura

        List<Soste> sosteList = this.bdhRequest.getSoste();
        StringBuilder bufferToAdd = new StringBuilder();
        int count = 0;

        for (Soste sostaTemp : sosteList) {
            String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(
                    sostaTemp.getTipoRiga(),
                    sostaTemp.getIstante(),
                    sostaTemp.getLatitudine(),
                    sostaTemp.getLongitudine(),
                    sostaTemp.getQualitaGps(),
                    sostaTemp.getProvincia(),
                    sostaTemp.getIndirizzo(),
                    sostaTemp.getCap(),
                    sostaTemp.getComune(),
                    sostaTemp.getCategoriaStrada(),
                    idClaim,
                    sostaTemp.getDriverUsername(),
                    sostaTemp.getDriverTaxId(),
                    sostaTemp.getFlagRiassegnato() != null
                            ? getIntFromStringBoolean(sostaTemp.getFlagRiassegnato())
                            : null
            );

            bufferToAdd.append(lineToAdd).append(NEW_LINE);
            count++;

            if (count % chunkSize == 0) {
                connectionHandler.insertBufferToDb(bufferToAdd, STG_SOSTE_ALFA, STG_SOSTE_ALFA_COL_NUM, chunkSize);
                bufferToAdd = new StringBuilder();
            }
        }

        if (bufferToAdd.length() > 0) {
            connectionHandler.insertBufferToDb(bufferToAdd, STG_SOSTE_ALFA, STG_SOSTE_ALFA_COL_NUM, chunkSize);
        }

        long endTime = System.nanoTime(); // fine misura
        long durationInMillis = (endTime - startTime) / 1_000_000; // converti in ms
        log.info("wrapInsertSoste executed in {} ms", durationInMillis);
    }

    public void wrapInsertViaggi(String idClaim) throws SQLException {
        long startTime = System.nanoTime();
        List<Viaggi> viaggiList = this.bdhRequest.getViaggi();
        StringBuilder bufferToAdd = new StringBuilder();

        int counter = 0;
        for (Viaggi viaggioTemp : viaggiList) {
            String indirizzoPartenza = viaggioTemp.getIndirizzoPartenza() != null
                    ? viaggioTemp.getIndirizzoPartenza().replaceAll(REPLACE_CHARACTERS_REGEX, " ") : "";
            String indirizzoArrivo = viaggioTemp.getIndirizzoArrivo() != null
                    ? viaggioTemp.getIndirizzoArrivo().replaceAll(REPLACE_CHARACTERS_REGEX, " ") : "";

            String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(
                    viaggioTemp.getTipoRiga(),
                    viaggioTemp.getIdViaggio(),
                    viaggioTemp.getIstanteAccensione(),
                    viaggioTemp.getIstanteSpegnimento(),
                    viaggioTemp.getLatitudineAccensione(),
                    viaggioTemp.getLongitudineAccensione(),
                    viaggioTemp.getQualitaGpsAccensione(),
                    viaggioTemp.getLatitudineSpegnimento(),
                    viaggioTemp.getLongitudineSpegnimento(),
                    viaggioTemp.getQualitaGpsSpegnimento(),
                    viaggioTemp.getMetriPercorsi(),
                    viaggioTemp.getProvinciaPartenza(),
                    indirizzoPartenza,
                    viaggioTemp.getCapPartenza(),
                    viaggioTemp.getComunePartenza(),
                    viaggioTemp.getCategoriaStradaPartenza(),
                    viaggioTemp.getProvinciaArrivo(),
                    indirizzoArrivo,
                    viaggioTemp.getCapArrivo(),
                    viaggioTemp.getComuneArrivo(),
                    viaggioTemp.getCategoriaStradaArrivo(),
                    idClaim,
                    viaggioTemp.getDriverUsername(),
                    viaggioTemp.getDriverTaxId(),
                    viaggioTemp.getFlagRiassegnato() != null ?
                            getIntFromStringBoolean(viaggioTemp.getFlagRiassegnato()) : null,
                    viaggioTemp.getTripid()
            );

            bufferToAdd.append(lineToAdd).append(NEW_LINE);
            counter++;
            if(counter % chunkSize == 0){
                connectionHandler.insertBufferToDb(bufferToAdd, STG_VIAGGIO_SOMMARIO_ALFA, STG_VIAGGIO_SOMMARIO_ALFA_COL_NUM, chunkSize);
                bufferToAdd = new StringBuilder();
            }
            // inserisce anche le posizioni intermedie associate
            log.trace("Wrap begin Viaggi posizioni into STG_VIAGGIO_POSIZIONI_ALFA for viaggio: {}", viaggioTemp.getIdViaggio());
            wrapInsertViaggiPosizioni(viaggioTemp.getPosizioniIntermedie(), idClaim, viaggioTemp.getIdViaggio());
            log.trace("Wrap end Viaggi posizioni into STG_VIAGGIO_POSIZIONI_ALFA for viaggio: {}", viaggioTemp.getIdViaggio());
        }

        // Insert finale batch
        connectionHandler.insertBufferToDb(bufferToAdd, STG_VIAGGIO_SOMMARIO_ALFA, STG_VIAGGIO_SOMMARIO_ALFA_COL_NUM, chunkSize);

        long endTime = System.nanoTime();
        long durationInMillis = (endTime - startTime) / 1_000_000;
        log.info("wrapInsertViaggi executed in {} ms", durationInMillis);
    }
    
    public void wrapInsertStatiDevice(String idClaim) throws SQLException {
        List<StatiDevice> statiDeviceList = this.bdhRequest.getStatiDevice();
        for (StatiDevice sdTemp : statiDeviceList) {
            String columnSql = utilityHandler.sqlGenerateColumnForQuery("TIPO_RIGA", "DATA_ORA_INIZIO", "DATA_ORA_FINE", "STATO_DEVICE","ID_CLAIM");
            String istanteInizio = StringUtils.isEmpty(sdTemp.getIstanteInizio()) ? null : sdTemp.getIstanteInizio();
            String istanteFine = StringUtils.isEmpty(sdTemp.getIstanteFine()) ? null : sdTemp.getIstanteFine();
            String valuesSql = utilityHandler.sqlGenerateValuesStringForQuery(sdTemp.getTipoRiga(), istanteInizio, istanteFine, sdTemp.getStatoDevice(),idClaim);
            String insertSqlComplete = connectionHandler.createInsertSql(STG_STATO_DEVICE_ALFA, columnSql, valuesSql);

            try {
                log.trace("Try insert into STG_STATO_DEVICE_ALFA query: " + insertSqlComplete);
                connectionHandler.executeUpdate(insertSqlComplete);
            } catch (SQLException e) {
                utilityHandler.handleExceptionLog("", e.getMessage(), e);
                throw new SQLException("Error insert stati device : " + insertSqlComplete + e.getMessage());
            }
        }
    }

    public void wrapInsertViaggiPosizioni(List<PosizioniIntermedie> posizioniIntermedieList, String idClaim, String idViaggio) throws SQLException {
        if (posizioniIntermedieList == null || posizioniIntermedieList.isEmpty()) {
            log.debug("No posizioniIntermedie to insert for viaggio: {}", idViaggio);
            return;
        }

        StringBuilder bufferToAdd = new StringBuilder();
        int counter = 0;
        for (PosizioniIntermedie posIntTemp : posizioniIntermedieList) {
            String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(
                    posIntTemp.getTipoRiga(),
                    posIntTemp.getIstante(),
                    posIntTemp.getLatitudine(),
                    posIntTemp.getLongitudine(),
                    posIntTemp.getQualitaGps(),
                    posIntTemp.getMomentoCrash(),
                    posIntTemp.getVelocitaGps(),
                    posIntTemp.getHeadingGps(),
                    posIntTemp.getStatoQuadro(),
                    posIntTemp.getDistanza(),
                    idClaim,
                    idViaggio
            );

            bufferToAdd.append(lineToAdd).append(NEW_LINE);
            counter++;
            if (counter % chunkSize == 0){
                connectionHandler.insertBufferToDb(bufferToAdd, STG_VIAGGIO_POSIZIONI_ALFA, STG_VIAGGIO_POSIZIONI_ALFA_COL_NUM, chunkSize);
                bufferToAdd = new StringBuilder();
            }
        }

        connectionHandler.insertBufferToDb(bufferToAdd, STG_VIAGGIO_POSIZIONI_ALFA, STG_VIAGGIO_POSIZIONI_ALFA_COL_NUM, chunkSize);
    }
	
    public void clearAllStaging() throws SQLException {
        try {
            String deleteTableSql = connectionHandler.createDeleteSql(STG_SEZIONE_GENERALE_ALFA);
            log.debug(CLEAR_LOG,STG_SEZIONE_GENERALE_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_CALIBRAZIONE_CRASH_ALFA);
            log.debug(CLEAR_LOG,STG_CALIBRAZIONE_CRASH_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_SOMMARIO_CRASH_ALFA);
            log.debug(CLEAR_LOG, STG_SOMMARIO_CRASH_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_VIAGGIO_SOMMARIO_ALFA);
            log.debug(CLEAR_LOG,STG_VIAGGIO_SOMMARIO_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_VIAGGIO_POSIZIONI_ALFA);
            log.debug(CLEAR_LOG,STG_VIAGGIO_POSIZIONI_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_POSIZIONI_CRASH_ALFA);
            log.debug(CLEAR_LOG,STG_POSIZIONI_CRASH_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_ANOMALIE_ALFA);
            log.debug(CLEAR_LOG,STG_ANOMALIE_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_SOSTE_ALFA);
            log.debug(CLEAR_LOG, STG_SOSTE_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_STATO_DEVICE_ALFA);
            log.debug(CLEAR_LOG,STG_STATO_DEVICE_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_GIROSCOPIO_CRASH_ALFA);
            log.debug(CLEAR_LOG,STG_GIROSCOPIO_CRASH_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

            deleteTableSql = connectionHandler.createDeleteSql(STG_ACCELERAZIONI_CRASH_ALFA);
            log.debug(CLEAR_LOG,STG_ACCELERAZIONI_CRASH_ALFA, deleteTableSql);
            connectionHandler.executeUpdate(deleteTableSql);

        } catch (SQLException e) {
            utilityHandler.handleExceptionLog("", "Clear Staging error : " + e.getMessage(), e);
            throw new SQLException("Error during clear staging table : " + e.getMessage());
        }
    }

    private String getIntFromStringBoolean(String value){
        return "true".equals(value) ? "1" : "0";
    }

}
