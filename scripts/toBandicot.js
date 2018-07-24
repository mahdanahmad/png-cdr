const fs	= require('fs');
const path	= require('path');
const async	= require('async');
const _		= require('lodash');
const csv	= require('fast-csv');

const root	= './data';
const dest	= './result';

if (!fs.existsSync(dest)) { fs.mkdirSync(dest); }
fs.readdirSync(root).filter(o => (path.extname(o) == '.csv')).forEach(file => {
	let filetime	= file.split('_')[0];

	let	csvStream	= csv.createWriteStream({ headers: true });
    let fsStream 	= fs.createWriteStream(dest + '/' + filetime + '_bandicot.csv');

	fsStream
		.on("pipe", () => { console.log(filetime + ' start!'); })
		.on("finish", () => { console.log(filetime + ' done!'); });

	csvStream.pipe(fsStream);
	fs.createReadStream(root + '/' + file)
		.pipe(csv({ headers: true }))
		.on("data", (data) => {
        	csvStream.write({
				interaction			: data['3'].substring(0,2) == 'MS' ? 'call' : (data['3'].substring(0,3) == 'SMS' ? 'text' : null),
				direction			: _.includes(data['3'], 'Originating') ? 'OUT' : (_.includes(data['3'], 'Terminating') ? 'IN' : null),
				correspondent_id	: parseInt(data['116']) || null,
				datetime			: data['19'].split('/').join('-') + ' ' + data['20'] || null,
				call_duration		: parseInt(data['22']) || null,
				antenna_id			: _.includes(data['3'], 'Originating') ? parseInt(data['119']) : (_.includes(data['3'], 'Terminating') ? parseInt(data['121']) : null),
			});
		})
		.on("end", () => { csvStream.end(); });
});
