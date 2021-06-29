module.exports.stringToJson = function(obj) {
  for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
          if (obj[key] && obj[key].length > 0) {
              try {
                  if (
                      key.toLowerCase().endsWith('userid') ||
                      key.toLowerCase().endsWith('dealershipid') ||
                      key.toLowerCase().endsWith('dealerid') ||
                      key.toLowerCase().endsWith('dealershipgroupid') ||
                      key.toLowerCase().endsWith('vehicleid') ||
                      key.toLowerCase().startsWith('saleschannelinstanceid') || key.toLowerCase().endsWith('saleschannelinstanceid') ||
                      key.toLowerCase().startsWith('saleschannelid') || key.toLowerCase().endsWith('saleschannelid') ||
                      key.toLowerCase().startsWith('saleschannelinstancevehicleid') || key.toLowerCase().endsWith('saleschannelinstancevehicleid') ||
                      key.toLowerCase().startsWith('stocknumber') || key.toLowerCase().endsWith('stocknumber') ||
                      key.toLowerCase().startsWith('interiorcolorid') || key.toLowerCase().endsWith('interiorcolorid') ||
                      key.toLowerCase().startsWith('licensenumber') || key.toLowerCase().endsWith('licensenumber') ||
                      key.toLowerCase().startsWith('zipcode') ||
                      key.toLowerCase().startsWith('vin') ||
                      key.toLowerCase().endsWith('name')
                  ) {
                      // do nothing, value should be string already
                  } else {
                      obj[key] = JSON.parse(obj[key]);
                  }
              } catch (e) {
                  // do nothing
              }
          }
      }
  }
  return obj;
};
