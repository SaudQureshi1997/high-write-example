import csv from "csv-parser";
import fs from "fs";
import { createObjectCsvWriter as createCsvWriter } from "csv-writer";
import mysql from "mysql2/promise";
import _ from "lodash";
import { exec } from "child_process";

const execute = (command, callback) => {
  exec(command, function (error, stdout, stderr) {
    callback(stdout);
  });
};

const connection = await mysql.createConnection({
  host: "0.0.0.0",
  user: "root",
  password: "saud1234",
  database: "high_write",
});

const prepareCsv = (callback) => {
  const contactsColumns = [
    { id: "First name", title: "First Name" },
    { id: "Last name", title: "Last Name" },
    { id: "Email", title: "Email" },
  ];

  const contactDetailsColumns = [
    { id: "contactId", title: "Contact Id" },
    { id: "Job", title: "Job" },
    { id: "Title", title: "Title" },
    { id: "Industry", title: "Industry" },
    { id: "Company", title: "Company" },
    { id: "City", title: "City" },
    { id: "State", title: "State" },
    { id: "Country", title: "Country" },
  ];

  const contactsWriter = createCsvWriter({
    path: "contacts.csv",
    header: contactsColumns,
  });
  const contactDetailsWriter = createCsvWriter({
    path: "contacts-details.csv",
    header: contactDetailsColumns,
  });
  let totalLength = 0;
  fs.createReadStream("dummy-1-million.csv")
    .pipe(csv())
    .on("data", async (data) => {
      const contacts = _.pick(data, ["First name", "Last name", "Email"]);
      const contactDetails = _.omit(data, ["First name", "Last name", "Email"]);
      contactDetails.contactId = totalLength + 1;
      totalLength += Object.values(data).length;
      await Promise.all([
        contactsWriter.writeRecords([contacts]),
        contactDetailsWriter.writeRecords([contactDetails]),
      ]);
    })
    .on("end", () => {
      console.log("Completed");
      callback();
    })
    .on("error", console.error);
};

const loadCsv = async () => {
  try {
    const query1 = `
        LOAD DATA INFILE '/var/lib/mysql-files/contacts.csv'\
        INTO TABLE contacts FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        IGNORE 1 LINES (firstName, lastName, email);
    `;

    const query2 = `
        LOAD DATA INFILE '/var/lib/mysql-files/contacts-details.csv'\
        INTO TABLE contact_details FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        IGNORE 1 LINES (contactId, job, title, industry, company, city, state, country);
    `;

    await Promise.all([connection.query(query1), connection.query(query2)]);
    console.log("Contacts Loaded!");
  } catch (err) {
    console.log(err);
  }
};

const main = async () => {
  console.time("prepareCsv");
  prepareCsv(() => {
    console.timeEnd("prepareCsv");
    execute("docker cp contacts.csv mysql:/var/lib/mysql-files", () => {
      console.log("contacts.csv copied!");
      execute(
        "docker cp contacts-details.csv mysql:/var/lib/mysql-files",
        async () => {
          console.log("contact-details.csv copied!");
          console.time("Loading CSV");
          await loadCsv();
          console.timeEnd("Loading CSV");
        }
      );
    });
  });
};

main();
