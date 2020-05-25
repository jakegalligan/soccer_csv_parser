const csv = require("csv-parser");
const fs = require("fs");
const {Pool, Client} = require("pg");
const { v4 } = require('uuid');


// connect to db client
const client = new Client({
    user: "postgres",
    host: "localhost",
    database: "postgres",
    password: "jg1996",
    port: 5432
});
client.connect();



// Create Tables
var cmd1 = `
CREATE TABLE Players(
    PLAYER_ID UUID PRIMARY KEY NOT NULL,
    Name TEXT NOT NULL,
    AGE INT NOT NULL,
    PHOTO TEXT,
    POSITION TEXT NOT NULL,
    JERSEY_NUMBER int NOT NULL,
    HEIGHT FLOAT,
    WEIGHT FLOAT,
    CLUB_ID UUID references clubs (club_id),
    NATION_ID UUID references nations (nation_id)
)
`;

cmd2 = `
CREATE TABLE Clubs (
    CLUB_ID UUID PRIMARY KEY NOT NULL,
    NAME text NOT NULL
)
`;

cmd3 = `
    CREATE TABLE PLAYER_RATINGS(
        PLAYER_RATING_ID UUID PRIMARY KEY NOT NULL,
        PLAYER_ID UUID REFERENCES Players (player_id) not null,
        Overall int not null,
        potential int not null,
        weak_foot int not null,
        skill_moves int not null,
        sprint_speed int not null,
        shot_power int not null,
        long_shots int not null,
        aggresssion int not null,
        dribbling int not null,
        fk_accuracy int not null,
        reaction int not null,
        strength int not null
    )
`;

cmd4 = `
CREATE TABLE Player_Contracts(
Player_Contract_Id UUID primary key not null,
Player_id UUID references players(Player_id) not null,
release_clause float,
contract_value_until date,
on_loan_from text,
joined date,
value float,
wage float
)
`;

var cmd5 = `
Create table nations(
Nation_id UUID Primary Key not null,
Nation text,
international_reputation int
)
`;


// run table commands
var commands = [];
commands.forEach(command => {
    client  
    .query(command)
    .then(res => {
        console.log(res);
    });
});


// parse  csv data
var results = [];
fs.createReadStream("../fifaDataFixed.csv")
    .pipe(csv())
    .on("data", (data) => results.push(data))
    .on('end', () => parseResults(results));

// create helper functions and variables

var clubs = {};
var clubChecker = (club) => {
    // if club exists grab existing data
    if (clubs[club]) {
        return {
            clubName: club,
            clubId: clubs[club],
            addToDB: false
        }
    }

    var newClubId = v4()
    clubs[club] = newClubId
    return {
        clubName: club,
        clubId: newClubId,
        addToDB: true
    }
}

var feetToInches = (height) => {
    var firstNumber = parseInt(height[0], 10);
    var feet = firstNumber * 12;
    var combined;
    if (height[3] != undefined){
        var combined = height[2] + height[3];
    } else {
        var combined = height[2];
    }

    var inches = parseInt(combined, 10);
    return feet + inches;
}

var freeAgentClubId = v4();

// parse results into csv
var parseResults = (results) => {
results.forEach(result => {
    var clubResults = clubChecker(result.Club);
    var clubName = clubResults.clubName;
    var clubId = clubResults.clubId;
    var addToDB = clubResults.addToDB;
    result.nationId = v4();
    result.playerRatingId = v4();
    result.playerContractId = v4();
    result.playerId = v4();
    var preValue = result.Value.substring(1);
    var value = preValue.slice(0,-1);
    var preWage = result.Wage.substring(1);
    var wage = preWage.slice(0,-1);
    var preReleaseClause = result.release_clause.substring(1);
    var releaseClause = preReleaseClause.slice(0, -1);
    var height = feetToInches(result.Height);
    var weight = result.Weight.slice(0,-3);

    // if club doesn't exist don't make a new one
    if (addToDB) insertClubs(clubId, clubName);
    insertNations(result.nationId, result.Nationality, result.international_reputation);
    insertPlayers(result.playerId,result.Name, result.Age, result.Photo, result.Position, result.jersey_number, height, weight, result.clubId, result.nationId)
    insertPlayerRatings(result.playerRatingId, result.playerId, result.Overall, result.Potential, result.weak_foot, result.skill_moves, result.sprint_speed, result.shot_power, result.long_shots, result.Aggression, result.Dribbling, result.fk_accuracy, result.Reaction, result.Strength);
    insertPlayerContracts(result.playerContractId, result.playerId,releaseClause, result.contract_value_until, result.on_loan_from, new Date(result.joined), value, wage);
})
}

var insertClubs = (clubId, clubName) => {
    if(!clubName) clubName ="Free Agent"
    if (!clubName) clubId = ""
    var cmd = `
    INSERT INTO CLUBS (club_id, name) VALUES($1, $2)
    `;

    var values = [clubId, clubName];

    client  
    .query(cmd, values)
    .then((err,res) => {
        if(err) console.log(err);
        if(res) console.log(res);
});
}

var insertNations = (nationId, nationName, reputation) => {
    var cmd = `
    INSERT INTO NATIONS(nation_id, nation, international_reputation) VALUES($1, $2, $3)
    `;
    var values = [nationId, nationName, reputation];

    client  
    .query(cmd, values)
    .then((err,res) => {
        if(err) console.log(err);
        if(res) console.log(res);
});
}

var insertPlayers = (playerId, name, age, photo, position, jerseyNumber, height, weight, clubId, nationId) => {
    var cmd = `
    INSERT INTO PLAYERS(PLAYER_ID,
    Name,
    AGE,
    PHOTO,
    POSITION,
    JERSEY_NUMBER,
    HEIGHT,
    WEIGHT,
    CLUB_ID,
    NATION_ID) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    `
    if (height == "") height = null;
    if (weight == "") weight = null;
    var values = [playerId,name, age, photo, position, jerseyNumber, height, weight, clubId, nationId];
    client  
    .query(cmd, values)
    .then((err,res) => {
        if(err) console.log(err);
        if(res) console.log(res);
});
}

var insertPlayerRatings = (playerRatingId,playerId, Overall, potential, weakFoot, skillMoves, sprintSpeed, shotPower, longShots, aggression, dribbling, fkAccuracy, reaction, strength) => {
    var cmd = `
    INSERT INTO PLAYER_RATINGS(PLAYER_RATING_ID,
        PLAYER_ID,
        Overall,
        potential,
        weak_foot,
        skill_moves,
        sprint_speed,
        shot_power,
        long_shots,
        aggresssion,
        dribbling,
        fk_accuracy,
        reaction,
        strength) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,$14)
    `;
var values = [playerRatingId, playerId, Overall, potential, weakFoot, skillMoves, sprintSpeed, shotPower, longShots, aggression, dribbling, fkAccuracy, reaction, strength];

    client  
    .query(cmd, values)
    .then((err,res) => {
        if(err) console.log(err);
        if(res) console.log(res);
});
}

var insertPlayerContracts = (playerContractId, playerId, release, contractValue, onLoanFrom, joined, value, wage) => {
    var cmd = `
    INSERT INTO PLAYER_CONTRACTS(
    Player_Contract_Id,
    Player_id,
    release_clause,
    contract_value_until,
    on_loan_from,
    joined,
    value,
    wage    
    ) VALUES($1, $2, $3, $4, $5, $6,$7,$8)
    `;
    if(value == "") value = null;
    if (wage == "") wage = null;
    if (release == "") release = null;
    var values = [playerContractId, playerId, release, contractValue, onLoanFrom, joined, value, wage];

    client  
    .query(cmd, values)
    .then((err, res) => {
        if(err) console.log(err);
        if(res) console.log(res);
        });

}


