const bluebird = require('bluebird');

module.exports = (function() {
    const eventstore = require('@saperiuminc/eventstore')({
        type: 'mysql',
        host: process.env.EVENTSTORE_MYSQL_HOST,
        port: process.env.EVENTSTORE_MYSQL_PORT,
        user: process.env.EVENTSTORE_MYSQL_USERNAME,
        password: process.env.EVENTSTORE_MYSQL_PASSWORD,
        database: process.env.EVENTSTORE_MYSQL_DATABASE,
        // projections-specific configuration below
        redisConfig: {
            host: process.env.REDIS_HOST,
            port: process.env.REDIS_PORT
        }, // required
        listStore: {
            host: process.env.EVENTSTORE_MYSQL_HOST,
            port: process.env.EVENTSTORE_MYSQL_PORT,
            user: process.env.EVENTSTORE_MYSQL_USERNAME,
            password: process.env.EVENTSTORE_MYSQL_PASSWORD,
            database: process.env.EVENTSTORE_MYSQL_DATABASE
        }, // required
        enableProjection: true,
        eventCallbackTimeout: 1000,
        pollingTimeout: 10000, // optional,
        pollingMaxRevisions: 5,
        errorMaxRetryCount: 2,
        errorRetryExponent: 2
    });

    bluebird.promisifyAll(eventstore);

    const initialize = async function() {
        try {
            await eventstore.initAsync();

            await eventstore.registerPlaybackListViewAsync('list_view_1', `
                SELECT
                    titles_list.row_id,
                    titles_list.row_revision,
                    titles_list.row_date,
                    titles_list.row_json->>'$.titleStatusId' as titleStatusId,
                    titles_list.row_json->>'$.paymentStatusId' as paymentStatusId,
                    titles_list.row_json->>'$.collectionStatusId' as collectionStatusId,
                    titles_list.row_json->>'$.collectionMethodId' as collectionMethodId,
                    titles_list.row_json->>'$.collectionDate' as collectionDate,
                    titles_list.row_json->'$.titleToBuyerStatusId' as titleToBuyerStatusId,
                    titles_list.row_json->>'$.receiveTrackingNumber' as receiveTrackingNumber,
                    titles_list.row_json->>'$.arbTrackingNumber' as arbTrackingNumber,
                    titles_list.row_json->>'$.sendTrackingNumber' as sendTrackingNumber,
                    titles_list.row_json->>'$.paymentTrackingNumber' as paymentTrackingNumber,
                    titles_list.row_json->>'$.vehicleStatusId' as vehicleStatusId,
                    titles_list.soldDealershipId as buyerDealershipId,
                    vehicle_list.dealershipId as sellerDealershipId,
                    vehicle_list.row_json->>'$.vin' as vin,
                    titles_list.row_json->>'$.soldDate' as soldDate,
                    titles_list.row_json->>'$.sortOrder' as sortOrder,
                    seller_dealership_list.row_json->>'$.sellerRepUserIds' as sellerRepUserIds, 
                    buyer_dealership_list.row_json->>'$.buyerRepUserIds' as buyerRepUserIds, 
                    JSON_SET(titles_list.row_json,
                        '$.vehicleStatusName', titles_list.row_json->>'$.vehicleStatusName',
                        '$.sellerPaidOnFaxApproved', false, 
                        '$.sellerDMVClerks', seller_dealership_list.row_json->'$.dmvClerks',
                        '$.sellerTitleAddressStreet', seller_dealership_list.row_json->>'$.titleAddressStreet',
                        '$.sellerTitleAddressZipCode', seller_dealership_list.row_json->>'$.titleAddressZipCode',
                        '$.sellerTitleAddressState', seller_dealership_list.row_json->>'$.titleAddressState',
                        '$.sellerTitleAddressCity', seller_dealership_list.row_json->>'$.titleAddressCity',
                        '$.titleStatusId', titles_list.row_json->>'$.titleStatusId', 
                        '$.titleStatusName', titles_list.row_json->>'$.titleStatusName', 
                        '$.titleReceivedAt', titles_list.row_json->'$.titleReceivedAt', 
                        '$.receiveTrackingNumber', titles_list.row_json->>'$.receiveTrackingNumber',  
                        '$.titleClerkFirstName', titles_list.row_json->>'$.titleClerkFirstName', 
                        '$.titleClerkLastName', titles_list.row_json->>'$.titleClerkLastName', 
                        '$.paymentStatusId', titles_list.row_json->>'$.paymentStatusId',  
                        '$.paymentStatusName', titles_list.row_json->>'$.paymentStatusName',  
                        '$.paymentTrackingNumber', titles_list.row_json->>'$.paymentTrackingNumber',  
                        '$.buyerTitleAddressStreet', buyer_dealership_list.row_json->>'$.titleAddressStreet',
                        '$.buyerTitleAddressZipCode', buyer_dealership_list.row_json->>'$.titleAddressZipCode',
                        '$.buyerTitleAddressState', buyer_dealership_list.row_json->>'$.titleAddressState',
                        '$.buyerTitleAddressCity', buyer_dealership_list.row_json->>'$.titleAddressCity',
                        '$.titleToBuyerStatusName', titles_list.row_json->>'$.titleToBuyerStatusName',  
                        '$.titleToBuyerStatusId', titles_list.row_json->'$.titleToBuyerStatusId',  
                        '$.sendTrackingNumber', titles_list.row_json->>'$.sendTrackingNumber',  
                        '$.collectionMethodId', titles_list.row_json->>'$.collectionMethodId',   
                        '$.collectionMethodName', titles_list.row_json->>'$.collectionMethodName',  
                        '$.collectionStatusId', titles_list.row_json->>'$.collectionStatusId',  
                        '$.collectionStatusName', titles_list.row_json->>'$.collectionStatusName',  
                        '$.cwReimbursementStatusId', titles_list.row_json->>'$.cwReimbursementStatusId',  
                        '$.cwReimbursementStatusName', titles_list.row_json->>'$.cwReimbursementStatusName',
                        '$.arbTrackingNumber', titles_list.row_json->>'$.arbTrackingNumber',  
                        '$.isPendingArbitration', titles_list.row_json->'$.isPendingArbitration', 
                        '$.soldDate', titles_list.row_json->>'$.soldDate',
                        '$.soldType', titles_list.row_json->>'$.soldType',
                        '$.unwoundAt', titles_list.row_json->'$.unwoundAt',
                        '$.vehicleId', vehicle_list.row_json->>'$.vehicleId',
                        '$.soldAmount', titles_list.row_json->>'$.soldAmount',
                        '$.sellerSoldAmount', titles_list.row_json->>'$.sellerSoldAmount',
                        '$.buyerSoldAmount', titles_list.row_json->>'$.buyerSoldAmount',
                        '$.buyerDealerId', buyer_user_list.row_json->>'$.userId',
                        '$.buyerAA5Million', '', 
                        '$.buyerReps', JSON_ARRAY(JSON_OBJECT('firstName', '', 'lastName', '')),
                        '$.buyerDealershipId', buyer_dealership_list.row_json->>'$.dealershipId',
                        '$.buyerDealerFirstName', buyer_user_list.row_json->>'$.firstName',
                        '$.buyerDealerLastName', buyer_user_list.row_json->>'$.lastName',
                        '$.buyerDealerMobileNumber', buyer_user_list.row_json->>'$.mobileNumber',
                        '$.buyerDealerEmail', buyer_user_list.row_json->>'$.email',
                        '$.buyerDealershipCity', buyer_dealership_list.row_json->>'$.city',
                        '$.buyerDealershipName', buyer_dealership_list.row_json->>'$.dealershipName',
                        '$.buyerDealershipState', buyer_dealership_list.row_json->>'$.state',
                        '$.soldPaymentMethodId', titles_list.row_json->>'$.soldPaymentMethodId',
                        '$.soldPaymentMethodName', titles_list.row_json->>'$.soldPaymentMethodName',
                        '$.buyerFacilitationFee', '', 
                        '$.buyerLicenseNumber', buyer_dealership_list.row_json->>'$.licenseNumber',
                        '$.buyerDealershipStreet', buyer_dealership_list.row_json->>'$.street',
                        '$.buyerDealershipZipCode', buyer_dealership_list.row_json->>'$.zipCode',
                        '$.salesChannelInstanceVehicleId', titles_list.row_json->>'$.salesChannelInstanceVehicleId', 
                        '$.updatedAt', '', 
                        '$.vin', vehicle_list.row_json->>'$.vin', 
                        '$.city', vehicle_list.row_json->>'$.city', 
                        '$.state', vehicle_list.row_json->>'$.state', 
                        '$.mileage', vehicle_list.row_json->>'$.mileage',
                        '$.zipCode', vehicle_list.row_json->>'$.zipCode',
                        '$.sellerDealerId', vehicle_list.row_json->>'$.dealerId',
                        '$.makeName', vehicle_list.row_json->>'$.makeName', 
                        '$.yearName', vehicle_list.row_json->>'$.yearName', 
                        '$.exteriorColor', vehicle_list.row_json->>'$.exteriorColor', 
                        '$.interiorColor', vehicle_list.row_json->>'$.interiorColor', 
                        '$.createdAt', '', 
                        '$.modelName', vehicle_list.row_json->>'$.modelName',
                        '$.sellerReps', JSON_ARRAY(JSON_OBJECT('firstName', '', 'lastName', '')),
                        '$.sellerDealershipId', seller_dealership_list.row_json->>'$.dealershipId',
                        '$.sellerDealershipName', seller_dealership_list.row_json->>'$.dealershipName',
                        '$.sellerDealerFirstName', seller_user_list.row_json->>'$.firstName',
                        '$.sellerDealerLastName', seller_user_list.row_json->>'$.lastName',
                        '$.sellerDealerMobileNumber', seller_user_list.row_json->>'$.mobileNumber',
                        '$.sellerDealerEmail', seller_user_list.row_json->>'$.email',
                        '$.sellerDealerLastName', seller_user_list.row_json->>'$.lastName',
                        '$.sellerLicenseNumber', seller_dealership_list.row_json->>'$.licenseNumber',
                        '$.sellerFirstPhotoPath', seller_user_list.row_json->>'$.photoPath',
                        '$.soldPaymentAdjustmentFees', titles_list.row_json->'$.soldPaymentAdjustmentFees',
                        '$.soldCollectionAdjustmentFees', titles_list.row_json->'$.soldCollectionAdjustmentFees',
                        '$.lastFollowUpNote', titles_list.row_json->>'$.lastFollowUpNote',
                        '$.lastFollowUpNoteCreatedAt', titles_list.row_json->'$.lastFollowUpNoteCreatedAt',
                        '$.lastRecapNote', titles_list.row_json->>'$.lastRecapNote',
                        '$.lastRecapNoteCreatedAt', titles_list.row_json->'$.lastRecapNoteCreatedAt',
                        '$.lastSellerResponse', titles_list.row_json->>'$.lastSellerResponse',
                        '$.lastSellerResponseCreatedAt', titles_list.row_json->'$.lastSellerResponseCreatedAt',
                        '$.buyFeeAmount', vehicle_list.row_json->'$.buyFeeAmount',
                        '$.saleFeeAmount', vehicle_list.row_json->'$.saleFeeAmount',
                        '$.soldAsDescribedGuarantee', vehicle_list.row_json->'$.soldAsDescribedGuarantee',
                        '$.collectionDate', titles_list.row_json->'$.collectionDate'
                    ) AS row_json
                FROM 
                    auction_titles_dashboard_vehicle_list titles_list
                JOIN
                    vehicle_vehicle_list vehicle_list  ON titles_list.vehicleId = vehicle_list.vehicleId
                LEFT JOIN
                    profile_dealership_list seller_dealership_list ON vehicle_list.dealershipId = seller_dealership_list.dealershipId
                LEFT JOIN
                    profile_dealership_list buyer_dealership_list ON titles_list.soldDealershipId = buyer_dealership_list.dealershipId
                LEFT JOIN
                    profile_user_list buyer_user_list ON titles_list.soldDealerId = buyer_user_list.userId
                LEFT JOIN
                    profile_user_list seller_user_list ON vehicle_list.dealerId = seller_user_list.userId
                WHERE vehicle_list.categoryType = 'trade-ins';
            `);

            await eventstore.registerPlaybackListViewAsync('list_view_2', `
                SELECT
                    titles_list.row_id,
                    titles_list.row_revision,
                    titles_list.row_date,
                    titles_list.row_json->>'$.titleStatusId' as titleStatusId,
                    titles_list.row_json->>'$.paymentStatusId' as paymentStatusId,
                    titles_list.row_json->>'$.collectionStatusId' as collectionStatusId,
                    titles_list.collectionMethodId as collectionMethodId,
                    titles_list.row_json->>'$.collectionDate' as collectionDate,
                    titles_list.row_json->'$.titleToBuyerStatusId' as titleToBuyerStatusId,
                    titles_list.row_json->>'$.receiveTrackingNumber' as receiveTrackingNumber,
                    titles_list.row_json->>'$.arbTrackingNumber' as arbTrackingNumber,
                    titles_list.row_json->>'$.sendTrackingNumber' as sendTrackingNumber,
                    titles_list.row_json->>'$.paymentTrackingNumber' as paymentTrackingNumber,
                    titles_list.row_json->>'$.vehicleStatusId' as vehicleStatusId,
                    titles_list.soldDealershipId as buyerDealershipId,
                    vehicle_list.dealershipId as sellerDealershipId,
                    vehicle_list.row_json->>'$.vin' as vin,
                    titles_list.soldDate as soldDate,
                    titles_list.sortOrder as sortOrder,
                    seller_dealership_list.sellerRepUserIds as sellerRepUserIds, 
                    buyer_dealership_list.buyerRepUserIds as buyerRepUserIds, 
                    JSON_SET(titles_list.row_json,
                        '$.vehicleStatusName', titles_list.row_json->>'$.vehicleStatusName',
                        '$.sellerPaidOnFaxApproved', false, 
                        '$.sellerDMVClerks', seller_dealership_list.row_json->'$.dmvClerks',
                        '$.sellerTitleAddressStreet', seller_dealership_list.row_json->>'$.titleAddressStreet',
                        '$.sellerTitleAddressZipCode', seller_dealership_list.row_json->>'$.titleAddressZipCode',
                        '$.sellerTitleAddressState', seller_dealership_list.row_json->>'$.titleAddressState',
                        '$.sellerTitleAddressCity', seller_dealership_list.row_json->>'$.titleAddressCity',
                        '$.titleStatusId', titles_list.row_json->>'$.titleStatusId', 
                        '$.titleStatusName', titles_list.row_json->>'$.titleStatusName', 
                        '$.titleReceivedAt', titles_list.row_json->'$.titleReceivedAt', 
                        '$.receiveTrackingNumber', titles_list.row_json->>'$.receiveTrackingNumber',  
                        '$.titleClerkFirstName', titles_list.row_json->>'$.titleClerkFirstName', 
                        '$.titleClerkLastName', titles_list.row_json->>'$.titleClerkLastName', 
                        '$.paymentStatusId', titles_list.row_json->>'$.paymentStatusId',  
                        '$.paymentStatusName', titles_list.row_json->>'$.paymentStatusName',  
                        '$.paymentTrackingNumber', titles_list.row_json->>'$.paymentTrackingNumber',  
                        '$.buyerTitleAddressStreet', buyer_dealership_list.row_json->>'$.titleAddressStreet',
                        '$.buyerTitleAddressZipCode', buyer_dealership_list.row_json->>'$.titleAddressZipCode',
                        '$.buyerTitleAddressState', buyer_dealership_list.row_json->>'$.titleAddressState',
                        '$.buyerTitleAddressCity', buyer_dealership_list.row_json->>'$.titleAddressCity',
                        '$.titleToBuyerStatusName', titles_list.row_json->>'$.titleToBuyerStatusName',  
                        '$.titleToBuyerStatusId', titles_list.row_json->'$.titleToBuyerStatusId',  
                        '$.sendTrackingNumber', titles_list.row_json->>'$.sendTrackingNumber',  
                        '$.collectionMethodId', titles_list.collectionMethodId,   
                        '$.collectionMethodName', titles_list.row_json->>'$.collectionMethodName',  
                        '$.collectionStatusId', titles_list.row_json->>'$.collectionStatusId',  
                        '$.collectionStatusName', titles_list.row_json->>'$.collectionStatusName',  
                        '$.cwReimbursementStatusId', titles_list.row_json->>'$.cwReimbursementStatusId',  
                        '$.cwReimbursementStatusName', titles_list.row_json->>'$.cwReimbursementStatusName',
                        '$.arbTrackingNumber', titles_list.row_json->>'$.arbTrackingNumber',  
                        '$.isPendingArbitration', titles_list.row_json->'$.isPendingArbitration', 
                        '$.soldDate', titles_list.soldDate,
                        '$.soldType', titles_list.row_json->>'$.soldType',
                        '$.unwoundAt', titles_list.row_json->'$.unwoundAt',
                        '$.vehicleId', vehicle_list.row_json->>'$.vehicleId',
                        '$.soldAmount', titles_list.row_json->>'$.soldAmount',
                        '$.sellerSoldAmount', titles_list.row_json->>'$.sellerSoldAmount',
                        '$.buyerSoldAmount', titles_list.row_json->>'$.buyerSoldAmount',
                        '$.buyerDealerId', buyer_user_list.row_json->>'$.userId',
                        '$.buyerAA5Million', '', 
                        '$.buyerReps', JSON_ARRAY(JSON_OBJECT('firstName', '', 'lastName', '')),
                        '$.buyerDealershipId', buyer_dealership_list.row_json->>'$.dealershipId',
                        '$.buyerDealerFirstName', buyer_user_list.row_json->>'$.firstName',
                        '$.buyerDealerLastName', buyer_user_list.row_json->>'$.lastName',
                        '$.buyerDealerMobileNumber', buyer_user_list.row_json->>'$.mobileNumber',
                        '$.buyerDealerEmail', buyer_user_list.row_json->>'$.email',
                        '$.buyerDealershipCity', buyer_dealership_list.row_json->>'$.city',
                        '$.buyerDealershipName', buyer_dealership_list.row_json->>'$.dealershipName',
                        '$.buyerDealershipState', buyer_dealership_list.row_json->>'$.state',
                        '$.soldPaymentMethodId', titles_list.row_json->>'$.soldPaymentMethodId',
                        '$.soldPaymentMethodName', titles_list.row_json->>'$.soldPaymentMethodName',
                        '$.buyerFacilitationFee', '', 
                        '$.buyerLicenseNumber', buyer_dealership_list.row_json->>'$.licenseNumber',
                        '$.buyerDealershipStreet', buyer_dealership_list.row_json->>'$.street',
                        '$.buyerDealershipZipCode', buyer_dealership_list.row_json->>'$.zipCode',
                        '$.salesChannelInstanceVehicleId', titles_list.row_json->>'$.salesChannelInstanceVehicleId', 
                        '$.updatedAt', '', 
                        '$.vin', vehicle_list.row_json->>'$.vin', 
                        '$.city', vehicle_list.row_json->>'$.city', 
                        '$.state', vehicle_list.row_json->>'$.state', 
                        '$.mileage', vehicle_list.row_json->>'$.mileage',
                        '$.zipCode', vehicle_list.row_json->>'$.zipCode',
                        '$.sellerDealerId', vehicle_list.row_json->>'$.dealerId',
                        '$.makeName', vehicle_list.row_json->>'$.makeName', 
                        '$.yearName', vehicle_list.row_json->>'$.yearName', 
                        '$.exteriorColor', vehicle_list.row_json->>'$.exteriorColor', 
                        '$.interiorColor', vehicle_list.row_json->>'$.interiorColor', 
                        '$.createdAt', '', 
                        '$.modelName', vehicle_list.row_json->>'$.modelName',
                        '$.sellerReps', JSON_ARRAY(JSON_OBJECT('firstName', '', 'lastName', '')),
                        '$.sellerDealershipId', seller_dealership_list.row_json->>'$.dealershipId',
                        '$.sellerDealershipName', seller_dealership_list.row_json->>'$.dealershipName',
                        '$.sellerDealerFirstName', seller_user_list.row_json->>'$.firstName',
                        '$.sellerDealerLastName', seller_user_list.row_json->>'$.lastName',
                        '$.sellerDealerMobileNumber', seller_user_list.row_json->>'$.mobileNumber',
                        '$.sellerDealerEmail', seller_user_list.row_json->>'$.email',
                        '$.sellerDealerLastName', seller_user_list.row_json->>'$.lastName',
                        '$.sellerLicenseNumber', seller_dealership_list.row_json->>'$.licenseNumber',
                        '$.sellerFirstPhotoPath', seller_user_list.row_json->>'$.photoPath',
                        '$.soldPaymentAdjustmentFees', titles_list.row_json->'$.soldPaymentAdjustmentFees',
                        '$.soldCollectionAdjustmentFees', titles_list.row_json->'$.soldCollectionAdjustmentFees',
                        '$.lastFollowUpNote', titles_list.row_json->>'$.lastFollowUpNote',
                        '$.lastFollowUpNoteCreatedAt', titles_list.row_json->'$.lastFollowUpNoteCreatedAt',
                        '$.lastRecapNote', titles_list.row_json->>'$.lastRecapNote',
                        '$.lastRecapNoteCreatedAt', titles_list.row_json->'$.lastRecapNoteCreatedAt',
                        '$.lastSellerResponse', titles_list.row_json->>'$.lastSellerResponse',
                        '$.lastSellerResponseCreatedAt', titles_list.row_json->'$.lastSellerResponseCreatedAt',
                        '$.buyFeeAmount', vehicle_list.row_json->'$.buyFeeAmount',
                        '$.saleFeeAmount', vehicle_list.row_json->'$.saleFeeAmount',
                        '$.soldAsDescribedGuarantee', vehicle_list.row_json->'$.soldAsDescribedGuarantee',
                        '$.collectionDate', titles_list.row_json->'$.collectionDate'
                    ) AS row_json
                FROM (
                    SELECT 
                        titles_list.row_id AS row1,
                        vehicle_list.row_id AS row2
                    FROM auction_titles_dashboard_vehicle_list titles_list
                    JOIN vehicle_vehicle_list vehicle_list ON vehicle_list.categoryType = 'trade-ins' AND titles_list.vehicleId = vehicle_list.vehicleId
                    LEFT JOIN profile_dealership_list seller_dealership_list ON vehicle_list.dealershipId = seller_dealership_list.dealershipId
                    LEFT JOIN profile_dealership_list buyer_dealership_list ON titles_list.soldDealershipId = buyer_dealership_list.dealershipId
                    @@where
                    @@order
                    @@limit
                ) LRLT
                JOIN auction_titles_dashboard_vehicle_list titles_list on LRLT.row1 = titles_list.row_id
                JOIN vehicle_vehicle_list vehicle_list on LRLT.row2 = vehicle_list.row_id
                LEFT JOIN profile_dealership_list seller_dealership_list ON vehicle_list.dealershipId = seller_dealership_list.dealershipId
                LEFT JOIN profile_dealership_list buyer_dealership_list ON titles_list.soldDealershipId = buyer_dealership_list.dealershipId
                LEFT JOIN profile_user_list buyer_user_list ON titles_list.soldDealerId = buyer_user_list.userId
                LEFT JOIN profile_user_list seller_user_list ON vehicle_list.dealerId = seller_user_list.userId
                @@order;

                SELECT COUNT(1) as total_count
                FROM auction_titles_dashboard_vehicle_list titles_list
                JOIN vehicle_vehicle_list vehicle_list ON vehicle_list.categoryType = 'trade-ins' AND titles_list.vehicleId = vehicle_list.vehicleId
                LEFT JOIN profile_dealership_list seller_dealership_list ON vehicle_list.dealershipId = seller_dealership_list.dealershipId
                LEFT JOIN profile_dealership_list buyer_dealership_list ON titles_list.soldDealershipId = buyer_dealership_list.dealershipId
                @@where;
            `, {
                alias: {
                    soldDate: `titles_list.soldDate`,
                    sellerRepUserIds: `seller_dealership_list.sellerRepUserIds`,
                    buyerRepUserIds: `buyer_dealership_list.buyerRepUserIds`,
                    collectionMethodId: `titles_list.collectionMethodId`,
                    sortOrder: `titles_list.sortOrder`,
                    sellerDealershipId: `vehicle_list.dealershipId`
                }
            });

            await eventstore.startAllProjectionsAsync();
        } catch (error) {
            console.error('error in setting up the projection', error);
        }
    }

    initialize();

    return eventstore;
})();