package it.unipol.alfa.cpm.portal.report.batch.service;

import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.request.*;
import it.unipol.alfa.cpm.portal.report.batch.constants.ParamConstants;
import it.unipol.alfa.cpm.portal.report.batch.constants.UtilityConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.File;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.Acceleration;
import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.Collision;
import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.Crash;
import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.CrashAnalysisResponse;
import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.Gyroscope;
import it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.response.Position;
import it.unipol.alfa.cpm.portal.report.batch.config.ConfigExternalLoader;
import it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.InputBdhRequest;
import it.unipol.alfa.cpm.portal.report.batch.util.ConnectionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;

import static it.unipol.alfa.cpm.portal.report.batch.constants.UtilityConstants.NEW_LINE;

@Slf4j
@Service
public class LoadResponseCrashAnalysisHandler {

	@Autowired
	private UtilityHandler utilityHandler;

	@Autowired
	private UtilityFileHandler utilityFileHandler;

	@Autowired
	private ConnectionHandler connectionHandler;

	@Autowired
	private ConfigExternalLoader configExternalLoader;

	private static final String STG_POST_CRASH_ACC = "STG_POST_CRASH_ACC";
	private static final int STG_POST_CRASH_ACC_COL_NUM = 8;
	private static final String STG_POST_CRASH_GYR = "STG_POST_CRASH_GYR";
	private static final int STG_POST_CRASH_GYR_COL_NUM = 5;
	private static final String STG_POST_CRASH_GPS = "STG_POST_CRASH_GPS";
	private static final int STG_POST_CRASH_GPS_COL_NUM = 7;
	private static String table = "POST_CRASH_PROCESS";
	private int chunkSize;

	private static final String LOG_CLAIM_AND_CRASH_ID = " for claim {} and crashId {}";
	private static final String ERROR_TRANSFORMING_DATE_LOG = "Error during transform {} with format {}";

	@PostConstruct
	public void init(){
		String chunkProperty = configExternalLoader.getProperty(ParamConstants.CHUNK_SIZE);
		if (chunkProperty != null){
			chunkSize = Integer.parseInt(chunkProperty);
		} else {
			chunkSize = 100;
		}
	}

	public void wrapResponseCAToDb(CrashAnalysisResponse crResponse) throws SQLException, NullPointerException {
		for (Crash crashTemp : crResponse.getCrashes()) {
			String crashId = crashTemp.getHeader().getCrashId();
			if (StringUtils.isEmpty(crashId) || StringUtils.isEmpty(crashTemp.getHeader().getClaimId())) {
				log.info("Skip crash empty or for return code : " + crashId);
			} else {
				log.info(
						"BEGIN call to put accelerations, gyroscopes, gps in stg tables and store procedures " + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashId);

				clearStgTables();
				insertCrashAnalysisResponseIntoDb(crashTemp);

				loadAndPrepareProcCrash(crashTemp);
				connectionHandler.commit();
				log.info(
						"END call to put accelerations, gyroscopes, gps in stg tables and store procedures "+ LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashId);
			}
		}
	}

	private void insertCrashAnalysisResponseIntoDb(Crash crashTemp) {
		insertiCrashAnalysisAccelerationtIntoDb(crashTemp.getAccelerations());
		insertCrashAnalysisPositionsIntoDb(crashTemp.getPositions());
		insertCrashAnalysisGyroIntoDb(crashTemp.getGyroscopes());
	}

	private void insertiCrashAnalysisAccelerationtIntoDb(List<Acceleration> accList) {
		StringBuilder bufferToAddAcc = new StringBuilder();
		try {
			int accCounter = 0;
			if (accList != null && !accList.isEmpty()) {
				for (Acceleration accTemp : accList) {
					log.info("BEGIN buffer for Acceleration : " + accTemp);

					String accX = accTemp.getAccX() == null ? "" : accTemp.getAccX().toString();
					String accY = accTemp.getAccY() == null ? "" : accTemp.getAccY().toString();
					String accZ = accTemp.getAccZ() == null ? "" : accTemp.getAccZ().toString();
					String accXY = accTemp.getAccXY() == null ? "" : accTemp.getAccXY().toString();
					String velAcc = accTemp.getVelAcc() == null ? "" : accTemp.getVelAcc().toString();
					String velGps = accTemp.getVelGps() == null ? "" : accTemp.getVelGps().toString();
					String crashIndex = accTemp.getCrashIndex() == null ? "" : accTemp.getCrashIndex().toString();
					String time = "";
					try {
						time = utilityHandler.transformStringDateForSqlOpz(accTemp.getTime(),
								UtilityConstants.DATE_FORMAT_YYYYMMDD_HH24_MI_SS_SSS,
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					} catch (ParseException e) {
						log.info(ERROR_TRANSFORMING_DATE_LOG, accTemp.getTime(),UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					}

					String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(time, accX, accY, accZ, accXY,
							velAcc, velGps, crashIndex);
					bufferToAddAcc.append(lineToAdd)
							.append(NEW_LINE);

					accCounter++;
					if (accCounter % chunkSize == 0){
						connectionHandler.insertBufferToDb(bufferToAddAcc, STG_POST_CRASH_ACC, STG_POST_CRASH_ACC_COL_NUM, chunkSize);
						bufferToAddAcc = new StringBuilder();
					}
				}
				connectionHandler.insertBufferToDb(bufferToAddAcc, STG_POST_CRASH_ACC, STG_POST_CRASH_ACC_COL_NUM, chunkSize);
			}
		} catch (SQLException e) {
			log.info("Exception while inserting CA accelerations: {}", e.getMessage());
		}

	}

	private void insertCrashAnalysisPositionsIntoDb(List<Position> gpsList) {
		StringBuilder bufferToAddGps = new StringBuilder();
		try {
			int posCounter = 0;
			if (gpsList != null && !gpsList.isEmpty()) {
				for (Position posTemp : gpsList) {
					log.info("BEGIN buffer for Position for GPS: " + posTemp);

					String lat = posTemp.getLatitude() == null ? "" : posTemp.getLatitude().toString();
					String lon = posTemp.getLongitude() == null ? "" : posTemp.getLongitude().toString();
					String velG = posTemp.getGpsVelocity() == null ? "" : posTemp.getGpsVelocity().toString();
					String quadro = posTemp.getIgnition() == null ? "" : posTemp.getIgnition().toString();
					String flag = posTemp.getIsCrash() == null ? "" : posTemp.getIsCrash().toString();
					String numOrigineDato = posTemp.getType() == null ? "" : posTemp.getType().toString();
					String time = "";
					try {
						time = utilityHandler.transformStringDateForSqlOpz(posTemp.getTime(),
								UtilityConstants.DATE_FORMAT_YYYYMMDD_HH24_MI_SS_SSS,
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					} catch (ParseException e) {
						log.info(ERROR_TRANSFORMING_DATE_LOG, posTemp.getTime(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					}

					String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(time, lat, lon, velG, quadro,
							flag, numOrigineDato);
					bufferToAddGps.append(lineToAdd).append(NEW_LINE);

					posCounter++;
					if (posCounter % chunkSize == 0){
						connectionHandler.insertBufferToDb(bufferToAddGps, STG_POST_CRASH_GPS, STG_POST_CRASH_GPS_COL_NUM, chunkSize);
						bufferToAddGps = new StringBuilder();
					}
				}
				connectionHandler.insertBufferToDb(bufferToAddGps, STG_POST_CRASH_GPS, STG_POST_CRASH_GPS_COL_NUM, chunkSize);
			}
		} catch (SQLException e) {
			log.info("Exception while inserting CA positions: {}", e.getMessage());
		}
	}

	private void insertCrashAnalysisGyroIntoDb(List<Gyroscope> gyrList){
		StringBuilder bufferToAddGyr = new StringBuilder();
		try {
			int gyrCounter = 0;
			if (gyrList != null && !gyrList.isEmpty()) {
				for (Gyroscope gyrTemp : gyrList) {
					log.info("BEGIN buffer for Gyroscopes for Gyroscope: " + gyrTemp);

					String velAngX = gyrTemp.getGyroX() == null ? "" : gyrTemp.getGyroX().toString();
					String velAngY = gyrTemp.getGyroY() == null ? "" : gyrTemp.getGyroY().toString();
					String velAngZ = gyrTemp.getGyroZ() == null ? "" : gyrTemp.getGyroZ().toString();
					String velAngMed = gyrTemp.getGyroXYZ() == null ? "" : gyrTemp.getGyroXYZ().toString();
					String time = "";
					try {
						time = utilityHandler.transformStringDateForSqlOpz(gyrTemp.getTime(),
								UtilityConstants.DATE_FORMAT_YYYYMMDD_HH24_MI_SS_SSS,
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					} catch (ParseException e) {
						log.info(ERROR_TRANSFORMING_DATE_LOG,gyrTemp.getTime(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS_SS);
					}
					String lineToAdd = utilityFileHandler.sqlGenerateValuesStringForFileCsv(time, velAngX, velAngY, velAngZ,
							velAngMed);
					bufferToAddGyr.append(lineToAdd).append(NEW_LINE);

					gyrCounter++;
					if (gyrCounter % chunkSize == 0){
						connectionHandler.insertBufferToDb(bufferToAddGyr, STG_POST_CRASH_GYR, STG_POST_CRASH_GYR_COL_NUM, chunkSize);
						bufferToAddGyr = new StringBuilder();
					}
				}
				connectionHandler.insertBufferToDb(bufferToAddGyr, STG_POST_CRASH_GYR, STG_POST_CRASH_GYR_COL_NUM, chunkSize);
			}
		} catch (SQLException e) {
			log.info("Exception while inserting CA gyro: {}", e.getMessage());
		}

	}

	private void loadAndPrepareProcCrash(Crash crashTemp) throws SQLException {
		// POST_CRASH_PROCESS
		String idElab = utilityHandler.obtainIdElaborazioneNext().toString();

		String nomeFile = "";
		String amax = crashTemp.getHeader().getAmax() == null ? "0" : crashTemp.getHeader().getAmax().toString();
		String theta = crashTemp.getHeader().getTheta() == null ? "0" : crashTemp.getHeader().getTheta().toString();
		String dvu = crashTemp.getHeader().getDvu() == null ? "0" : crashTemp.getHeader().getDvu().toString();
		String vu = crashTemp.getHeader().getVu() == null ? "0" : crashTemp.getHeader().getVu().toString();
		String ragione = "0";
		String iIniAni = "0";
		String iUrtoAni = "0";
		String iFineAni = "0";
		String respLiv1 = "0";
		String respLiv2 = "0";
		String gpsAct = crashTemp.getHeader().getGpsActivity() == null ? "0"
				: crashTemp.getHeader().getGpsActivity().toString();
		String lat = crashTemp.getHeader().getLatitude() == null ? "0" : crashTemp.getHeader().getLatitude().toString();
		String lon = crashTemp.getHeader().getLongitude() == null ? "0"
				: crashTemp.getHeader().getLongitude().toString();
		String vGps = crashTemp.getHeader().getVGps() == null ? "0" : crashTemp.getHeader().getVGps().toString();
		String accMse = crashTemp.getHeader().getAccMse() == null ? "0" : crashTemp.getHeader().getAccMse().toString();
		String preCrashAccT = crashTemp.getHeader().getPreCrashAccT() == null ? "0"
				: crashTemp.getHeader().getPreCrashAccT().toString();
		String preCrashAccN = crashTemp.getHeader().getPreCrashAccN() == null ? "0"
				: crashTemp.getHeader().getPreCrashAccN().toString();
		String collisionCount = crashTemp.getHeader().getCollisionCount() == null ? "0"
				: crashTemp.getHeader().getCollisionCount().toString();
		String dvun = crashTemp.getHeader().getDvun() == null ? "0" : crashTemp.getHeader().getDvun().toString();
		String crashDuration = crashTemp.getHeader().getCrashDuration() == null ? "0"
				: crashTemp.getHeader().getCrashDuration().toString();
		String numClaimOrder = crashTemp.getHeader().getNumClaimOrder() == null ? "0"
				: crashTemp.getHeader().getNumClaimOrder().toString();
		String thetaGyro = crashTemp.getHeader().getThetaGyro() == null ? "0"
				: crashTemp.getHeader().getThetaGyro().toString();
		String collisionOffset = crashTemp.getHeader().getCollisionOffset() == null ? "0"
				: crashTemp.getHeader().getCollisionOffset().toString();
		String driverUsername = crashTemp.getHeader().getDriverUsername() == null ? ""
				: crashTemp.getHeader().getDriverUsername();
		String driverTaxId = crashTemp.getHeader().getDriverTaxId() == null ? ""
				: crashTemp.getHeader().getDriverTaxId();
		String velGpsValid = crashTemp.getHeader().getIsVelGpsValid() == null ? ""
				: crashTemp.getHeader().getIsVelGpsValid();
		String velAccValid = crashTemp.getHeader().getIsVelAccValid() == null ? ""
				: crashTemp.getHeader().getIsVelAccValid();

		// ELABORAZIONE_DETTAGLIO_URTI_PROCESS
		String impulseDvu;
		String ppu0;
		String ppu1;
		String ppu2;
		String eventDuration;

		log.info("BEGIN call to POST_CRASH_PROCESS " + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashTemp.getHeader().getCrashId());
		log.info("Data crash analysis: " + crashTemp.getHeader().getCrashDate() + " for sx: "
				+ crashTemp.getHeader().getClaimId());

		String data = crashTemp.getHeader().getCrashDate();
		try {

			String crashDatePad = StringUtils.rightPad(crashTemp.getHeader().getCrashDate(),
					23, '0');
			data = utilityHandler.transformStringDateForSql(crashDatePad,
					UtilityConstants.DATE_FORMAT_YYYYMMDD_HH24_MI_SS_SSS, UtilityConstants.DATE_FORMAT_DD_MM_YYYY);
		} catch (ParseException e) {
			log.info(ERROR_TRANSFORMING_DATE_LOG, crashTemp.getHeader().getCrashDate(), UtilityConstants.DATE_FORMAT_DD_MM_YYYY);
		}

		String ora = crashTemp.getHeader().getCrashDate();
		try {
			String crashOraPad = StringUtils.rightPad(crashTemp.getHeader().getCrashDate(), 23,
					'0');
			ora = utilityHandler.transformStringDateForSql(crashOraPad,
					UtilityConstants.DATE_FORMAT_YYYYMMDD_HH24_MI_SS_SSS, UtilityConstants.TIME_FORMAT_HH24_MI_SS_SSS);
		} catch (ParseException e) {
			log.info(ERROR_TRANSFORMING_DATE_LOG, crashTemp.getHeader().getCrashDate(), UtilityConstants.TIME_FORMAT_HH24_MI_SS_SSS);
		}

		String command = "call " + UtilityConstants.UNICO_SCHEMA + "." + table + "(";
		command += idElab + ",";
		command += "," + crashTemp.getHeader().getEngineVersion() == null ? ""
				: crashTemp.getHeader().getEngineVersion();
		command += ",'" + data + "'";
		command += ",'" + ora + "'";
		command += ",'" + "2" + "'";
		command += ",'" + nomeFile + "'";
		command += ",\'" + (crashTemp.getHeader().getCrashId() == null ? "" : crashTemp.getHeader().getCrashId())
				+ "\'";
		command += ",\'" + amax + "\'";
		command += ",\'" + theta + "\'";
		command += ",\'" + dvu + "\'";
		command += ",\'" + vu + "\'";
		command += ",\'" + ragione + "\'";
		command += ",\'" + gpsAct + "\'";
		command += ",\'" + lat + "\'";
		command += ",\'" + lon + "\'";
		command += ",\'" + iIniAni + "\'";
		command += ",\'" + iUrtoAni + "\'";
		command += ",\'" + iFineAni + "\'";
		command += ",\'" + (crashTemp.getHeader().getClaimId() == null ? "" : crashTemp.getHeader().getClaimId())
				+ "\'";
		command += ",\'" + (crashTemp.getHeader().getSuccess() == null ? "" : crashTemp.getHeader().getSuccess())
				+ "\'";
		command += ",\'" + (crashTemp.getHeader().getRetCode() == null ? "" : crashTemp.getHeader().getRetCode())
				+ "\'";
		command += ",\'"
				+ (crashTemp.getHeader().getMotionDirection() == null ? "" : crashTemp.getHeader().getMotionDirection())
				+ "\'";
		command += "," + stNum(vGps);
		command += "," + stNum(accMse);
		command += "," + stNum(preCrashAccT);
		command += "," + stNum(preCrashAccN);
		command += "," + stNum(collisionCount);
		command += ",\'" + (crashTemp.getHeader().getCollisionDirection() == null ? ""
				: crashTemp.getHeader().getCollisionDirection()) + "\'";
		command += "," + stNum(dvun);
		command += "," + stNum(crashDuration);
		command += "," + stNum(respLiv1);
		command += "," + stNum(respLiv2);
		command += "," + stNum(numClaimOrder);
		command += "," + stNum(thetaGyro);
		command += "," + stNum(collisionOffset);
		command += ",\'" + driverUsername + "\'";
		command += ",\'" + driverTaxId + "\'";
		command += ",\'" + velGpsValid + "\'";
		command += ",\'" + velAccValid + "\'";
		command += ")";
		log.info("Prepared Execution to POST_CRASH_PROCESS" + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashTemp.getHeader().getCrashId());
		connectionHandler.executeProcedureParam(command);
		log.info("END call to POST_CRASH_PROCESS" + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(),crashTemp.getHeader().getCrashId());

		// nel codice batch è¨ indice
		int i = 1;

		log.info("BEGIN call to ELABORAZIONE_DETTAGLIO_URTI_PROCESS" + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashTemp.getHeader().getCrashId());

		if (crashTemp.getHeader().getCollisions() == null) {
			log.info("Collisions are empty for : " + crashTemp.getHeader().getClaimId());
		} else {
			for (Collision collTemp : crashTemp.getHeader().getCollisions()) {
				List<String> valuesInElabDettUrti = new ArrayList<>();

				amax = collTemp.getAmax() == null ? "0" : collTemp.getAmax().toString();
				theta = collTemp.getTheta() == null ? "0" : collTemp.getTheta().toString();
				impulseDvu = collTemp.getImpulseDvu() == null ? "0" : collTemp.getImpulseDvu().toString();
				ppu0 = collTemp.getPpu0() == null ? "0" : collTemp.getPpu0().toString();
				ppu1 = collTemp.getPpu1() == null ? "0" : collTemp.getPpu1().toString();
				ppu2 = collTemp.getPpu2() == null ? "0" : collTemp.getPpu2().toString();
				eventDuration = collTemp.getEventDuration() == null ? "0" : collTemp.getEventDuration().toString();

				valuesInElabDettUrti.add(idElab);
				valuesInElabDettUrti.add(String.valueOf(i));
				valuesInElabDettUrti.add(amax);
				valuesInElabDettUrti.add(theta);
				valuesInElabDettUrti.add(collTemp.getCollisionDirection());
				valuesInElabDettUrti.add(impulseDvu);
				valuesInElabDettUrti.add(ppu0);
				valuesInElabDettUrti.add(ppu1);
				valuesInElabDettUrti.add(ppu2);
				valuesInElabDettUrti.add(eventDuration);

				log.info("Prepared Execution to ELABORAZIONE_DETTAGLIO_URTI_PROCESS" + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashTemp.getHeader().getCrashId());
				i++;
				connectionHandler.prepareAndExecuteProcedure("ELABORAZIONE_DETTAGLIO_URTI_PROCESS",
						valuesInElabDettUrti);
			}
		}
		log.info("END call to ELABORAZIONE_DETTAGLIO_URTI_PROCESS" + LOG_CLAIM_AND_CRASH_ID, crashTemp.getHeader().getClaimId(), crashTemp.getHeader().getCrashId());
	}

	public CrashAnalysisRequest wrapRequestCAToObj(InputBdhRequest bdhRequest) {

		CrashAnalysisRequest requestCA = new CrashAnalysisRequest();
		String dataUltimoDownloadDati = bdhRequest.getDataUltimoDownloadDati();
		String istanteRichiesta = bdhRequest.getIstanteRichiesta();
		if (bdhRequest.getDataUltimoDownloadDati() != null && !bdhRequest.getDataUltimoDownloadDati().equals("")) {
			try {
				dataUltimoDownloadDati = utilityHandler.transformStringDateForSql(
						bdhRequest.getDataUltimoDownloadDati(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
						UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
			} catch (ParseException e) {
				log.info(ERROR_TRANSFORMING_DATE_LOG, bdhRequest.getDataUltimoDownloadDati(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
			}
		}
		if (bdhRequest.getIstanteRichiesta() != null && !bdhRequest.getIstanteRichiesta().equals("")) {
			try {
				istanteRichiesta = utilityHandler.transformStringDateForSql(bdhRequest.getIstanteRichiesta(),
						UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS, UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
			} catch (ParseException e) {
				log.info(ERROR_TRANSFORMING_DATE_LOG, bdhRequest.getIstanteRichiesta(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
			}
		}
		log.info("BEGIN wrap for claimId: " + bdhRequest.getClaimId());

		requestCA.setHWModel(bdhRequest.getHWModel() == null ? "" : bdhRequest.getHWModel());
		requestCA.setClaimId(bdhRequest.getClaimId() == null ? "" : bdhRequest.getClaimId());
		requestCA.setDataUltimoDownloadDati(dataUltimoDownloadDati);
		requestCA.setEsito(bdhRequest.getEsito() == null ? "" : bdhRequest.getEsito());
		requestCA.setFreqAcc(bdhRequest.getFreqAcc() == null ? "" : bdhRequest.getFreqAcc());
		requestCA.setFreqGps(bdhRequest.getFreqGps() == null ? "" : bdhRequest.getFreqGps());
		requestCA.setFreqGyro(bdhRequest.getFreqGyro() == null ? "" : bdhRequest.getFreqGyro());
		requestCA.setIstanteRichiesta(istanteRichiesta);
		requestCA.setModulo(bdhRequest.getModulo() == null ? "" : bdhRequest.getModulo());
		requestCA.setNumeroAnomalie(bdhRequest.getNumeroAnomalie() == null ? 0 : bdhRequest.getNumeroAnomalie());
		requestCA.setNumeroCrashTrovati(
				bdhRequest.getNumeroCrashTrovati() == null ? 0 : bdhRequest.getNumeroCrashTrovati());
		requestCA.setNumeroPeriodiMancataAssociazione(bdhRequest.getNumeroPeriodiMancataAssociazione() == null ? ""
				: bdhRequest.getNumeroPeriodiMancataAssociazione());
		requestCA.setNumeroViaggiTrovati(
				bdhRequest.getNumeroViaggiTrovati() == null ? 0 : bdhRequest.getNumeroViaggiTrovati());
		requestCA.setProduttoreBlackBox(
				bdhRequest.getProduttoreBlackBox() == null ? "" : bdhRequest.getProduttoreBlackBox());
		requestCA.setTarga(bdhRequest.getTarga() == null ? "" : bdhRequest.getTarga());
		requestCA.setTelefono1(bdhRequest.getTelefono1() == null ? "ND" : bdhRequest.getTelefono1());
		requestCA.setTelefono2(bdhRequest.getTelefono2() == null ? "ND" : bdhRequest.getTelefono2());
		requestCA.setTelefono3(bdhRequest.getTelefono3() == null ? "ND" : bdhRequest.getTelefono3());
		requestCA.setTelefono4(bdhRequest.getTelefono4() == null ? "ND" : bdhRequest.getTelefono4());
		requestCA.setTipoContratto(bdhRequest.getTipoContratto() == null ? "" : bdhRequest.getTipoContratto());
		requestCA.setTipoRiga(bdhRequest.getTipoRiga() == null ? "" : bdhRequest.getTipoRiga());
		requestCA.setTipologiaDispositivo(
				bdhRequest.getTipologiaDispositivo() == null ? "" : bdhRequest.getTipologiaDispositivo());
		requestCA.setVersFirmware(bdhRequest.getVersFirmware() == null ? "" : bdhRequest.getVersFirmware());

		log.info(
				"HEADER added for claimId: " + bdhRequest.getClaimId());

		List<it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.request.Crash> crashes = new ArrayList<>();

		for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Crash bdhCrash : bdhRequest.getCrashes()) {

			String istanteCrash = bdhCrash.getIstanteCrash();

			it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.request.Crash crash = new it.unipol.alfa.cpm.portal.report.batch.client.crash.dto.request.Crash();

			if (bdhCrash.getIstanteCrash() != null && !bdhCrash.getIstanteCrash().equals("")) {
				try {
					istanteCrash = utilityHandler.transformStringDateForSql(bdhCrash.getIstanteCrash(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhCrash.getIstanteCrash(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			crash.setAccPrimoUrto(bdhCrash.getAccPrimoUrto());
			crash.setAccSecondoUrto(bdhCrash.getAccSecondoUrto());
			crash.setAccTerzoUrto(bdhCrash.getAccTerzoUrto());
			crash.setAccelerazione(bdhCrash.getAccelerazione());
			crash.setAlgoritmoRilevazione(bdhCrash.getAlgoritmoRilevazione());
			crash.setAngoloPrimoUrto(bdhCrash.getAngoloPrimoUrto());
			crash.setAngoloSecondoUrto(bdhCrash.getAngoloSecondoUrto());
			crash.setAngoloTerzoUrto(bdhCrash.getAngoloTerzoUrto());
			crash.setCap(bdhCrash.getCap());
			crash.setCategoriaStrada(bdhCrash.getCategoriaStrada());
			crash.setCodiceCompagnia(bdhCrash.getCodiceCompagnia());
			crash.setCodiceInstallatore(bdhCrash.getCodiceInstallatore());
			crash.setComune(bdhCrash.getComune());
			crash.setCrashAssistenza(bdhCrash.getCrashAssistenza());
			crash.setCrashAttivazione(bdhCrash.getCrashAttivazione());
			crash.setCrashFiltrato(bdhCrash.getCrashFiltrato());
			crash.setCrashRicevuto(bdhCrash.getCrashRicevuto());
			crash.setDataRegistrazione(bdhCrash.getDataRegistrazione());
			crash.setDeviceAttivo(bdhCrash.getDeviceAttivo());
			crash.setIdAssistenza(bdhCrash.getIdAssistenza());
			crash.setIdCrash(bdhCrash.getIdCrash());
			crash.setIndirizzo(bdhCrash.getIndirizzo());
			crash.setIstanteCrash(istanteCrash);
			crash.setLatitudine(bdhCrash.getLatitudine());
			crash.setLongitudine(bdhCrash.getLongitudine());
			crash.setMatriceRotazione(new MatriceRotazione());
			crash.getMatriceRotazione().setAlfaX(bdhCrash.getMatriceRotazione().getAlfaX());
			crash.getMatriceRotazione().setAlfaY(bdhCrash.getMatriceRotazione().getAlfaY());
			crash.getMatriceRotazione().setAlfaZ(bdhCrash.getMatriceRotazione().getAlfaZ());
			crash.getMatriceRotazione().setOffsetX(bdhCrash.getMatriceRotazione().getOffsetX());
			crash.getMatriceRotazione().setOffsetY(bdhCrash.getMatriceRotazione().getOffsetY());
			crash.getMatriceRotazione().setOffsetZ(bdhCrash.getMatriceRotazione().getOffsetZ());
			crash.getMatriceRotazione().setTipoRiga(bdhCrash.getMatriceRotazione().getTipoRiga());
			crash.setModulo(bdhCrash.getModulo());
			crash.setProvincia(bdhCrash.getProvincia());
			crash.setQualitaGps(bdhCrash.getQualitaGps());
			crash.setRagioneSocialeInstallatore(bdhCrash.getRagioneSocialeInstallatore());
			crash.setSensoMarcia(bdhCrash.getSensoDiMarcia().getStato());
			crash.setTempoSecondoUrto(bdhCrash.getTempoSecondoUrto());
			crash.setTempoTerzoUrto(bdhCrash.getTempoTerzoUrto());
			crash.setTipoRiga(bdhCrash.getTipoRiga());
			crash.setTipologiaCrash(bdhCrash.getTipologiaCrash());
			crash.setDriverUsername(bdhCrash.getDriverUsername());
			crash.setDriverTaxId(bdhCrash.getDriverTaxId());

			crashes.add(crash);

			log.info("Crash added for idCrash: " + bdhCrash.getIdCrash());

			List<Posizioni> positions = new ArrayList<>();

			for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Posizioni bdhPosizione : bdhCrash.getPosizioni()) {

				String istantePosition = bdhPosizione.getIstante();

				if (bdhPosizione.getIstante() != null && !bdhPosizione.getIstante().equals("")) {
					try {
						istantePosition = utilityHandler.transformStringDateForSql(bdhPosizione.getIstante(),
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
					} catch (ParseException e) {
						log.info(ERROR_TRANSFORMING_DATE_LOG, bdhPosizione.getIstante(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
					}
				}
				Posizioni position = new Posizioni();
				position.setDistanza(bdhPosizione.getDistanza());
				position.setHeadingGps(bdhPosizione.getHeadingGps());
				position.setIstante(istantePosition);
				position.setLatitudine(bdhPosizione.getLatitudine());
				position.setLongitudine(bdhPosizione.getLongitudine());
				position.setPosizioneCrash(bdhPosizione.getPosizioneCrash());
				position.setQualitaGps(bdhPosizione.getQualitaGps());
				position.setStatoQuadro(bdhPosizione.getStatoQuadro());
				position.setTipoRiga(bdhPosizione.getTipoRiga());

				position.setVelocitaGps(bdhPosizione.getVelocitaGps());

				positions.add(position);

				log.info("Position added for idCrash: " + bdhCrash.getIdCrash());

				List<Accelerazioni> accelerations = new ArrayList<>();

				for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Accelerazioni bdhAccelerazione : bdhPosizione
						.getAccelerazioni()) {
					Accelerazioni acceleration = new Accelerazioni();
					acceleration.setAccX(bdhAccelerazione.getAccX());
					acceleration.setAccY(bdhAccelerazione.getAccY());
					acceleration.setAccZ(bdhAccelerazione.getAccZ());
					acceleration.setTipoRiga(bdhAccelerazione.getTipoRiga());

					accelerations.add(acceleration);
					log.info("Acceleration added for idCrash: " + bdhCrash.getIdCrash());
				}

				position.setAccelerazioni(accelerations);

				List<Giroscopus> gyroscopes = new ArrayList<>();

				for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Giroscopus bdhGyroscope : bdhPosizione
						.getGiroscopi()) {
					Giroscopus gyroscope = new Giroscopus();

					gyroscope.setGyroX(bdhGyroscope.getGyroX());
					gyroscope.setGyroY(bdhGyroscope.getGyroY());
					gyroscope.setGyroZ(bdhGyroscope.getGyroZ());
					gyroscope.setTipoRiga(bdhGyroscope.getTipoRiga());

					gyroscopes.add(gyroscope);
					log.trace("Gyroscope added for idCrash: " + bdhCrash.getIdCrash());
				}

				position.setGiroscopi(gyroscopes);
			}

			crash.setPosizioni(positions);
		}
		requestCA.setCrashes(crashes);

		List<Soste> soste = new ArrayList<>();

		for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Soste bdhSosta : bdhRequest.getSoste()) {
			Soste sosta = new Soste();
			String istanteSoste = bdhSosta.getIstante();

			if (bdhSosta.getIstante() != null && !bdhSosta.getIstante().equals("")) {
				try {
					istanteSoste = utilityHandler.transformStringDateForSql(bdhSosta.getIstante(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhSosta.getIstante(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			sosta.setCap(bdhSosta.getCap());
			sosta.setCategoriaStrada(bdhSosta.getCategoriaStrada());
			sosta.setComune(bdhSosta.getComune());
			sosta.setIndirizzo(bdhSosta.getIndirizzo());
			sosta.setIstante(istanteSoste);
			sosta.setLatitudine(bdhSosta.getLatitudine());
			sosta.setLongitudine(bdhSosta.getLongitudine());
			sosta.setProvincia(bdhSosta.getProvincia());
			sosta.setQualitaGps(bdhSosta.getQualitaGps());
			sosta.setTipoRiga(bdhSosta.getTipoRiga());
			sosta.setDriverUsername(bdhSosta.getDriverUsername());
			sosta.setDriverTaxId(bdhSosta.getDriverTaxId());

			soste.add(sosta);
			log.info(
					"Soste added for idClaim: " + bdhRequest.getClaimId());
		}

		requestCA.setSoste(soste);

		List<StatiDevice> statiDevice = new ArrayList<>();
		String istanteFineDevice = "";
		String istanteInizioDevice = "";

		for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.StatiDevice bdhStatoDevice : bdhRequest.getStatiDevice()) {
			StatiDevice statoDevice = new StatiDevice();

			istanteFineDevice = bdhStatoDevice.getIstanteFine();
			istanteInizioDevice = bdhStatoDevice.getIstanteInizio();

			if (bdhStatoDevice.getIstanteFine() != null && !bdhStatoDevice.getIstanteFine().equals("")) {
				try {
					istanteFineDevice = utilityHandler.transformStringDateForSql(bdhStatoDevice.getIstanteFine(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhStatoDevice.getIstanteFine(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			if (bdhStatoDevice.getIstanteInizio() != null && !bdhStatoDevice.getIstanteInizio().equals("")) {
				try {
					istanteInizioDevice = utilityHandler.transformStringDateForSql(bdhStatoDevice.getIstanteInizio(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhStatoDevice.getIstanteInizio(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}

			statoDevice.setIstanteFine(istanteFineDevice);
			statoDevice.setIstanteInizio(istanteInizioDevice);
			statoDevice.setStatoDevice(bdhStatoDevice.getStatoDevice());
			statoDevice.setTipoRiga(bdhStatoDevice.getTipoRiga());

			statiDevice.add(statoDevice);
			log.info("StatoDevice added for idClaim: " + bdhRequest.getClaimId());
		}

		requestCA.setStatiDevice(statiDevice);

		List<Anomalie> anomalie = new ArrayList<>();

		String istanteFineAnomalie = "";
		String istanteInizioAnomalie = "";

		for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Anomalie bdhAnomalia : bdhRequest.getAnomalie()) {
			Anomalie anomalia = new Anomalie();

			if (bdhAnomalia.getIstanteInizio() != null && !bdhAnomalia.getIstanteInizio().equals("")) {
				try {
					istanteInizioAnomalie = utilityHandler.transformStringDateForSql(bdhAnomalia.getIstanteInizio(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhAnomalia.getIstanteInizio(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			if (bdhAnomalia.getIstanteFine() != null && !bdhAnomalia.getIstanteFine().equals("")) {
				try {
					istanteFineAnomalie = utilityHandler.transformStringDateForSql(bdhAnomalia.getIstanteFine(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhAnomalia.getIstanteFine(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}

			anomalia.setTipoRiga(bdhAnomalia.getTipoRiga());
			anomalia.setTipoAnomalia(bdhAnomalia.getTipoAnomalia());
			anomalia.setStatoAnomalia(bdhAnomalia.getStatoAnomalia());
			anomalia.setIstanteInizio(istanteInizioAnomalie);
			anomalia.setIstanteFine(istanteFineAnomalie);

			anomalie.add(anomalia);
			log.info("Anomalia added for idClaim: " + bdhRequest.getClaimId());
		}

		requestCA.setAnomalie(anomalie);

		List<Viaggi> viaggi = new ArrayList<>();

		for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.Viaggi bdhViaggio : bdhRequest.getViaggi()) {
			Viaggi viaggio = new Viaggi();

			String istanteAccensione = bdhViaggio.getIstanteAccensione();
			String istanteSpegnimento = bdhViaggio.getIstanteSpegnimento();

			if (bdhViaggio.getIstanteAccensione() != null && !bdhViaggio.getIstanteAccensione().equals("")) {
				try {
					istanteAccensione = utilityHandler.transformStringDateForSql(bdhViaggio.getIstanteAccensione(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhViaggio.getIstanteAccensione(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			if (bdhViaggio.getIstanteSpegnimento() != null && !bdhViaggio.getIstanteSpegnimento().equals("")) {
				try {
					istanteSpegnimento = utilityHandler.transformStringDateForSql(bdhViaggio.getIstanteSpegnimento(),
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
							UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				} catch (ParseException e) {
					log.info(ERROR_TRANSFORMING_DATE_LOG, bdhViaggio.getIstanteSpegnimento(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
				}
			}
			viaggio.setCapArrivo(bdhViaggio.getCapArrivo());
			viaggio.setCapPartenza(bdhViaggio.getCapPartenza());
			viaggio.setCategoriaStradaArrivo(bdhViaggio.getCategoriaStradaArrivo());
			viaggio.setCategoriaStradaPartenza(bdhViaggio.getCategoriaStradaPartenza());
			viaggio.setComuneArrivo(bdhViaggio.getComuneArrivo());
			viaggio.setComunePartenza(bdhViaggio.getComunePartenza());
			viaggio.setIdViaggio(bdhViaggio.getIdViaggio());
			viaggio.setIndirizzoArrivo(bdhViaggio.getIndirizzoArrivo());
			viaggio.setIndirizzoPartenza(bdhViaggio.getIndirizzoPartenza());
			viaggio.setIstanteAccensione(istanteAccensione);
			viaggio.setIstanteSpegnimento(istanteSpegnimento);
			viaggio.setLatitudineAccensione(bdhViaggio.getLatitudineAccensione());
			viaggio.setLatitudineSpegnimento(bdhViaggio.getLatitudineSpegnimento());
			viaggio.setLongitudineAccensione(bdhViaggio.getLongitudineAccensione());
			viaggio.setLongitudineSpegnimento(bdhViaggio.getLongitudineSpegnimento());
			viaggio.setMetriPercorsi(bdhViaggio.getMetriPercorsi());
			viaggio.setProvinciaArrivo(bdhViaggio.getProvinciaArrivo());
			viaggio.setProvinciaPartenza(bdhViaggio.getProvinciaPartenza());
			viaggio.setQualitaGpsAccensione(bdhViaggio.getQualitaGpsAccensione());
			viaggio.setQualitaGpsSpegnimento(bdhViaggio.getQualitaGpsSpegnimento());
			viaggio.setTipoRiga(bdhViaggio.getTipoRiga());
			viaggio.setDriverUsername(bdhViaggio.getDriverUsername());
			viaggio.setDriverTaxId(bdhViaggio.getDriverTaxId());

			viaggi.add(viaggio);

			log.info("Viaggio added for idClaim: " + bdhRequest.getClaimId());

			List<PosizioniIntermedie> posizioniIntermedie = new ArrayList<>();

			for (it.unipol.alfa.cpm.portal.report.batch.domain.dto.bdh.PosizioniIntermedie bdhPosizioneIntermedia : bdhViaggio
					.getPosizioniIntermedie()) {
				PosizioniIntermedie posizioneIntermedia = new PosizioniIntermedie();

				String istanteIntermedio = bdhPosizioneIntermedia.getIstante();

				if (bdhPosizioneIntermedia.getIstante() != null && !bdhPosizioneIntermedia.getIstante().equals("")) {
					try {
						istanteIntermedio = utilityHandler.transformStringDateForSql(
								bdhPosizioneIntermedia.getIstante(), UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS,
								UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
					} catch (ParseException e) {
						log.info(ERROR_TRANSFORMING_DATE_LOG, bdhPosizioneIntermedia.getIstante(),  UtilityConstants.DATE_FORMAT_DD_MM_YY_HH24_MI_SS);
					}
				}
				posizioneIntermedia.setDistanza(bdhPosizioneIntermedia.getDistanza());
				posizioneIntermedia.setHeadingGps(bdhPosizioneIntermedia.getHeadingGps());
				posizioneIntermedia.setIstante(istanteIntermedio);
				posizioneIntermedia.setLatitudine(bdhPosizioneIntermedia.getLatitudine());
				posizioneIntermedia.setLongitudine(bdhPosizioneIntermedia.getLongitudine());
				posizioneIntermedia.setMomentoCrash(bdhPosizioneIntermedia.getMomentoCrash());
				posizioneIntermedia.setQualitaGps(bdhPosizioneIntermedia.getQualitaGps());
				posizioneIntermedia.setStatoQuadro(bdhPosizioneIntermedia.getStatoQuadro());
				posizioneIntermedia.setTipoRiga(bdhPosizioneIntermedia.getTipoRiga());
				posizioneIntermedia.setVelocitaGps(bdhPosizioneIntermedia.getVelocitaGps());

				posizioniIntermedie.add(posizioneIntermedia);

				log.info("PosizioneIntermedia added for idClaim: " + bdhRequest.getClaimId());
			}
			viaggio.setPosizioniIntermedie(posizioniIntermedie);
		}
		requestCA.setViaggi(viaggi);

		requestCA.setStatiDevice(new ArrayList<>());

		log.info("END wrap for claimId: " + bdhRequest.getClaimId());

		return requestCA;
	}

	private String stNum(String from) {
		return (from == null || from.length() == 0) ? "0" : from;
	}

	private void clearStgTables() throws SQLException {
		try {
			String clearTableSql = connectionHandler.createDeleteSql(STG_POST_CRASH_ACC);
			log.info("Try clear STG_POST_CRASH_ACC query: " + clearTableSql);
			connectionHandler.executeUpdate(clearTableSql);

			clearTableSql = connectionHandler.createDeleteSql(STG_POST_CRASH_GYR);
			log.info("Try clear STG_POST_CRASH_GYR query: " + clearTableSql);
			connectionHandler.executeUpdate(clearTableSql);

			clearTableSql = connectionHandler.createDeleteSql(STG_POST_CRASH_GPS);
			log.info("Try clear STG_POST_CRASH_GPS query: " + clearTableSql);
			connectionHandler.executeUpdate(clearTableSql);

			connectionHandler.commit();

		} catch (SQLException e) {
			utilityHandler.handleExceptionLog("", "Clear Staging error : " + e.getMessage(), e);
			throw new SQLException("Error during clear staging table : " + e.getMessage());
		}
	}
}
