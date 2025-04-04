import fs from "fs";
import jose from "node-jose";
import { randomUUID } from "crypto";
import axios from "axios";
import dotenv from "dotenv";
import readline from "readline";
import nodemailer from "nodemailer";
dotenv.config();

const clientId = process.env.CLIENT_ID;
const tokenEndpoint = process.env.TOKEN_ENDPOINT;
const fhirBaseUrl = process.env.FHIR_BASE_URL;
const groupId = "e3iabhmS8rsueyz7vaimuiaSmfGvi.QwjVXJANlPOgR83";

const createJWT = async (payload) => {
  const ks = fs.readFileSync("keys.json");
  const keyStore = await jose.JWK.asKeyStore(ks.toString());
  const key = keyStore.get({ use: "sig" });
  return jose.JWS.createSign({ compact: true, fields: { typ: "JWT" } }, key)
    .update(JSON.stringify(payload))
    .final();
};

const generateExpiry = (minutes) => {
  return (new Date().getTime() + minutes * 60 * 1000) / 1000; // 4 minutes
};
const makeTokenRequest = async () => {
  const jwt = await createJWT({
    iss: clientId,
    sub: clientId,
    aud: tokenEndpoint,
    jti: randomUUID(),
    exp: generateExpiry(4),
  });

  console.log(jwt);
  const formParams = new URLSearchParams();
  formParams.set("grant_type", "client_credentials");
  formParams.set(
    "client_assertion_type",
    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
  );
  formParams.set("client_assertion", jwt);
  console.log(tokenEndpoint);
  const tokenResponse = await axios.post(tokenEndpoint, formParams, {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
  });
  return tokenResponse.data;
};

const kickOffBulkDataExport = async (accessToken) => {
  const bulkKickofResponse = await axios.get(
    `${fhirBaseUrl}/Group/${groupId}/$export`,
    {
      params: {
        _type: "patient,observation",
        _typeFilter: "Observation?category=laboratory",
      },
      headers: {
        Accept: "application/fhir+json",
        Authorization: `Bearer ${accessToken}`,
        Prefer: "respond-async",
      },
    }
  );

  return bulkKickofResponse.headers.get("content-location");
};

const pollAndWaitforExport = async (url, accessToken) => {
  const response = await axios.get(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });

  if (response.status === 202) {
    // still processing
    const progress = response.headers["x-progress"];
    console.log(`waiting for export to complete: ${progress}`);
    await new Promise((resolve) => setTimeout(resolve, 5000));
    return await pollAndWaitforExport(url, accessToken);
  } else if (response.status === 200) {
    return response.data;
  } else {
    throw new Error(`Unexpected status: ${response.status}`);
  }
};

const processBulkResponse = async (bundleResponse, accessToken) => {
  const promises = bundleResponse.output?.map(async (output) => {
    const url = output.url;
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
      responseType: "stream",
    });

    const rl = readline.createInterface({
      input: response.data,
      crlfDelay: Infinity,
    });

    const resources = [];
    for await (const line of rl) {
      if (line.trim() !== "") {
        const resource = JSON.parse(line);
        resources.push(resource);
      }
    }

    return {
      url,
      type: output.type,
      resources,
    };
  });
  // Wait for all NDJSON files to be fully processed before closing the file
  return Promise.all(promises);
};

const findAbnormalLabs = (observations) => {
  const abnormalByPatient = {};

  observations.forEach((obs) => {
    const { subject, code, valueQuantity, referenceRange } = obs;

    if (!subject?.reference || !valueQuantity || !referenceRange?.length)
      return;

    const patientId = subject.reference.replace("Patient/", "");
    const { value, unit } = valueQuantity;
    const range = referenceRange[0];
    const low = range.low?.value;
    const high = range.high?.value;

    if (
      (low !== undefined && value < low) ||
      (high !== undefined && value > high)
    ) {
      if (!abnormalByPatient[patientId]) abnormalByPatient[patientId] = [];

      abnormalByPatient[patientId].push({
        test: code?.text || code?.coding?.[0]?.display || "Unknown Test",
        value,
        unit,
        reference: `${low ?? "-"} - ${high ?? "-"}`,
      });
    }
  });
  console.log(abnormalByPatient);
  return abnormalByPatient;
};

const getAllLabsByPatient = (observations) => {
  const labsByPatient = {};

  observations.forEach((obs) => {
    const { subject, code, valueQuantity, referenceRange } = obs;

    if (!subject?.reference || !valueQuantity) return;

    const patientId = subject.reference.replace("Patient/", "");
    const { value, unit } = valueQuantity;
    const range = referenceRange?.[0];
    const low = range?.low?.value;
    const high = range?.high?.value;

    if (!labsByPatient[patientId]) labsByPatient[patientId] = [];

    let isAbnormal = false;
    if (low !== undefined && value < low) {
      isAbnormal = true;
    } else if (high !== undefined && value > high) {
      isAbnormal = true;
    }

    labsByPatient[patientId].push({
      test: code?.text || code?.coding?.[0]?.display || "Unknown Test",
      value,
      unit,
      reference:
        low !== undefined || high !== undefined
          ? `${low ?? "-"} - ${high ?? "-"}`
          : "Reference range not available",
      abnormal: isAbnormal,
    });
  });

  return labsByPatient;
};

const getPatientNames = (patientResources) => {
  const namesByPatientId = {};
  patientResources.forEach((patient) => {
    const id = patient.id;
    const nameEntry = patient.name?.[0];

    if (!id || !nameEntry) return;

    const fullName = [...(nameEntry.given || []), nameEntry.family].join(" ");

    namesByPatientId[id] = fullName;
  });
  return namesByPatientId;
};

const alertPatients = async (labsByPatient, patientNames) => {
  for (const patientId in labsByPatient) {
    const labs = labsByPatient[patientId];
    const patientName = patientNames[patientId] || "Unknown Patient";

    const summary = labs
      .map(
        (lab) =>
          `${lab.test}: ${lab.value} ${lab.unit || ""} (Ref: ${
            lab.reference
          }) ${lab.abnormal ? "! ABNORMAL" : ""}`
      )
      .join("\n");

    await sendEmail(
      process.env.ALERT_EMAIL_TO,
      `Lab Results for ${patientName} ${patientId}`,
      summary
    );
  }
};

const sendEmail = async (to, subject, text) => {
  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASS,
    },
  });

  const mailOptions = {
    from: `"FHIR Alerts" <${process.env.EMAIL_USER}>`,
    to,
    subject,
    text,
  };

  try {
    await transporter.sendMail(mailOptions);
    console.log(`Email sent to ${to}`);
  } catch (error) {
    console.error("Email failed:", error.message);
  }
};

const tokenResponse = await makeTokenRequest();
const accessToken = tokenResponse.access_token;
const contentLocation = await kickOffBulkDataExport(accessToken);
const bulkDataResponse = await pollAndWaitforExport(
  contentLocation,
  accessToken
);
const bulkData = await processBulkResponse(bulkDataResponse, accessToken);
const labResources =
  bulkData.find((r) => r.type === "Observation")?.resources || [];
const patientResources =
  bulkData.find((r) => r.type === "Patient")?.resources || [];
const patientLabs = getAllLabsByPatient(labResources);
const patientNames = getPatientNames(patientResources);
await alertPatients(patientLabs, patientNames);
