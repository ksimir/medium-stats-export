/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.exportMediumStats = (req, res) => {
  const puppeteer = require('puppeteer');
  const {Storage} = require('@google-cloud/storage');
  const Readable = require('stream').Readable;
  const url = "https://medium.com/m/signin?redirect=https%3A%2F%2Fmedium.com%2F&operation=login";
  
  // Method to autoscroll through the stats page
  const autoScroll = page =>
    page.evaluate(
      async () =>
        await new Promise((resolve, reject) => {
          let totalHeight = 0;
          let distance = 100;
          let timer = setInterval(() => {
            let scrollHeight = document.body.scrollHeight;
            window.scrollBy(0, distance);
            totalHeight += distance;
            if (totalHeight >= scrollHeight) {
              clearInterval(timer);
              resolve();
            }
          }, 300);
        })
    );

  // Method to update text data to GCS file
  const uploadPromise = (text) => {
    return new Promise((resolve, reject) => {
      const storageClient = new Storage();
      const bucket = storageClient.bucket(process.env.MEDIUM_BUCKET);
      var filename = "test/stats-medium-metrics-" + new Date().toISOString().slice(0, 10) + ".csv";
      const uploadFile = bucket.file(filename);
      const uploadStream = uploadFile.createWriteStream({
        predefinedAcl: 'publicRead',
        metadata: {
          cacheControl: 'no-cache',
          contentType: 'text/plain',
        },
      });

      const readStream = new Readable();
      readStream.push(text);
      readStream.push(null);

      readStream
        .on('error', reject)
        .pipe(uploadStream)
        .on('error', reject)
        .on('finish', resolve);
    });
  };

  // Main function that will scrap Medium stats page using puppeteer
  async function main() {
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox'],
    });
    const page = await browser.newPage({ context: 'another-context' });
    await page.goto(url, { waitUntil: "networkidle2" });
    console.log('Clicking on google login...');
    await page.click('.button--withChrome.button--large');
    await page.mainFrame().waitForSelector('#identifierId');
    console.log('Typing email...');
    await page.type('#identifierId', process.env.MEDIUM_USERNAME);
    await page.mainFrame().waitForSelector('#identifierNext');
    console.log('Clicking next button...');
    await page.click('#identifierNext');
    console.log('Waiting for password field...');
    await page
      .mainFrame()
      .waitForSelector('#password input[type="password"]', {visible: true});
    console.log('Typing password...');
    await page.type('#password input[type="password"]', process.env.MEDIUM_PASSWORD, {
      delay: 100,
    });
    console.log('Clicking sign in button...');
    await Promise.all([
  	  page.waitForNavigation({waitUntil: "domcontentloaded"}),
  	  page.click('#passwordNext', {delay: 100})
	]);
    
    await page.waitFor(3000);
    
    // This section verifies that the redirection has finished and medium main page loaded
    try {
      await page.waitForFunction(
        'document.querySelector("body").innerText.includes("HOME")',
  	  );
      console.log('Medium home page successfully loaded!');
    } catch(err) {
      console.log('Medium home page has not loaded...');
    }
      
    // This section verifies if you get asked to fill out a recovery email address after logging in
    try {
      const verify = await page.evaluate('document.querySelector("body").innerText.includes("Verify it\'s you")');
      if (verify) {  
        console.log('Recovery email page detected!');

        // Filling out your recovery email
        await (await page.$('input')).type(process.env.RECOVERY_EMAIL);
        await page.keyboard.press('Enter');

        console.log('Wait 3 seconds');
        await page.waitFor(3000);
      } else {
        console.log('Recovery email page not detected...');
      }
    } catch(err) {
      console.log(err);
    }

    await page.waitFor(3000);

    console.log('Opening the medium stats page...');
    await page.goto('https://medium.com/google-cloud-jp/stats/stories', { waitUntil: "networkidle2" });
    console.log('Autoscrolling...');
    await autoScroll(page);
    console.log('Reached the end of the page...');
    
    // Displayed logs from within page.evaluate()
    page.on('console', (log) => console[log._type](log._text));
    
    // Scrape the medium stats page and return the entire stats
    var content = await page.evaluate(async() => {
      return await new Promise((resolve, reject) => {
        try {
          var URLBits = document.URL;

          console.log('Exporting stats...');
          var statType = document.querySelectorAll("h1")[0];
          var statsfrom = "";

          var content = "mediumID|title|link|publication|mins|views|reads|readRatio|fans|pubDate|liveDate"+ "\n";
          var rows = document.querySelectorAll(".sortableTable-row.js-statsTableRow");
          rows.forEach(function(row) {
            var mediumID = row.getAttribute("data-action-value");
            var title = row.querySelectorAll(".sortableTable-title a")[0].innerText;
            var publicationtxt = row.querySelectorAll("a.sortableTable-link")[0].innerText;
            if(publicationtxt == "View story"){
              if(statType.innerText == "Stats"){
                var publication = "Not in publication";

              } else {
                var publicationStats = document.querySelectorAll("h1.hero-title")[0].innerText;
                var publication = publicationStats.substring(0,publicationStats.lastIndexOf(" stats"));

              };
              var link = row.querySelectorAll("a.sortableTable-link")[0].href;
            } else {
              var publication = publicationtxt;
              var link = row.querySelectorAll("a.sortableTable-link")[1].href;

            };
            var mins = row.querySelectorAll("span.readingTime")[0].title;
            var values = row.querySelectorAll(".sortableTable-value");
            var views = values[1].innerText;
            var reads = values[2].innerText;
            var readRatio = values[3].innerText;
            var fans = values[4].innerText;
            var pubDate = new Date(parseInt(row.getAttribute("data-timestamp"))).toISOString().slice(0, 10);
            var liveDate = new Date(parseInt(values[0].innerText)).toISOString().slice(0, 10);
            content += mediumID + "|" + title + "|" + link + "|" + publication + "|" + mins + "|" + views + "|" + reads + "|" + readRatio + "|" + fans + "|" + pubDate + "|" + liveDate +"\n";
          });
          resolve(content);
        } catch (err) {
          console.log(err);
          reject(err.toString());
        }
      });
    });

    // Update medium stats data in CSV format to GCS
    await uploadPromise(content);
    console.log('Export done!');

    console.log('Closing browser...');
    await browser.close();
  }

  main()
  .then(url => {
    console.log("Successfully uploaded medium stats to GCS");
    res.status(200).send("Success");
  })
  .catch(err => {
    console.error(err);
    res.status(500).send("An Error occured" + err);  
  })
};
