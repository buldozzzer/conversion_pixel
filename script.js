async function sendImpressions(){

    const userLang = navigator.language || navigator.userLanguage;

    const browser = (function(){
        var ua= navigator.userAgent;
        var tem; 
        var M= ua.match(/(opera|chrome|safari|firefox|msie|trident(?=\/))\/?\s*(\d+)/i) || [];
        if(/trident/i.test(M[1])){
            tem=  /\brv[ :]+(\d+)/g.exec(ua) || [];
            return 'IE '+(tem[1] || '');
        }
        if(M[1]=== 'Chrome'){
            tem= ua.match(/\b(OPR|Edge)\/(\d+)/);
            if(tem!= null) return tem.slice(1).join(' ').replace('OPR', 'Opera');
        }
        M= M[2]? [M[1], M[2]]: [navigator.appName, navigator.appVersion, '-?'];
        if((tem= ua.match(/version\/(\d+)/i))!= null) M.splice(1, 1, tem[1]);
        return M.join(' ');
    })();

    const country = Intl.DateTimeFormat().resolvedOptions().locale

    const os = (function() {
        if (typeof navigator.userAgentData !== 'undefined' && navigator.userAgentData != null) {
            return navigator.userAgentData.platform;
        }
        return 'unknown';
    })();

    const myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");

    const raw = JSON.stringify({
        "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc", //заменить
        "os": os,
        "browser": browser,
        "country": country,
        "language": userLang,
        "compaign_id": "compaign_id", //заменить
        "source_id": "source_id", //заменить
        "zone_id": "zone_id" //заменить
    });

    const requestOptions = {
    method: "POST",
    headers: myHeaders,
    body: raw,
    redirect: "follow"
    };

    fetch("https://144.126.130.252/impressions", requestOptions)
    .then((response) => response.text())
    .then((result) => console.log(result))
    .catch((error) => console.error(error));
};

async function sendConversions(){
    const myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");

    const raw = JSON.stringify({
    "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc", //заменить
    "revenue": "revenue", //заменить
    "var": "var" //заменить
    });

    const requestOptions = {
        method: "POST",
        headers: myHeaders,
        body: raw,
        redirect: "follow"
    };

    fetch("https://144.126.130.252/conversions", requestOptions)
    .then((response) => response.text())
    .then((result) => console.log(result))
    .catch((error) => console.error(error));
}

sendConversions();
