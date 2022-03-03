const zbench = require('@saperiuminc/zbench');
const compose = require('docker-compose');
const mysql2 = require('mysql2/promise');
const path = require('path');
const bluebird = require('bluebird');
const debug = require('debug')('DEBUG');
const configs = require('./config');
const Redis = require('ioredis')
// const shortid = require('shortid');

const shardCount = +process.env.SHARD_COUNT || 1;
let config = {
    clusters: configs.clusterPorts.slice(0, shardCount).map((port) => {
        const esConfig = JSON.parse(JSON.stringify((configs.esConfig)));
        esConfig.port = port;
        return esConfig;
    })
};

const redisFactory = require('./redis-factory');
config = Object.assign(config, configs.projectionConfigs);
config.redisCreateClient = redisFactory.createClient

// console.log(config);

const testAggregateIds = [
    'nP0UTpzfn',
    'LqMnBxVueN',
    'qqdRe1nKlT',
    'XWBtUKdPIP',
    'fSQSQS6i6c',
    'Tt1BXhPVH7',
    'BDuXMp6zJU',
    'FKJWffpzbk',
    'u4IX2ANvq3',
    '_RPcM5BeJQ',
    'NNsAp1KyPP',
    'wR_dhCNKea',
    'k-MHvzUrTM',
    'jprpq2vUBY',
    'RtoH1KN0s1',
    'X_b3dk7LNo',
    'xZckSLj9Jry',
    'LMb6mvMSMjG',
    'kDExZH899jw',
    'VD5s0PO4PvA',
    '_xVQ7klTkEi',
    'cPjutAgSDvj',
    'yu5xllonMxE',
    'OiIGIKjfUEY',
    '3A3XSDaGHwa',
    '3stoslou8V_',
    'a1Uw4wCXUjL',
    'tLU7x8a3Cnz',
    'Uj6thJWTaLj',
    'UUd1FOMZQSi',
    'b50ARZD1Ni5',
    '1XyskyRC3ng',
    's2pgdVwkxhq',
    '6iaEBdXmDs6',
    'iYcEnNk_-RG',
    'Fs121_lvwov',
    'wG2W6OTVktO',
    'g43ips-_Spv',
    'dBOnAMrznzC',
    'pm_Q9-3TykL',
    'pDpOdKQb31J',
    'H_UaUxepvLz',
    'QOAd0R1yKY8',
    'a8WQMhdxcMU',
    'ZKK-E8I4K65',
    'EdMAzsyS1aa',
    '2zlQb0TfKVK',
    'Q4BAfux3bH9',
    'dlWvkKD9TET',
    'oh6nwroiBaF',
    'xjzJeZTgLPt',
    'PeYwRpTujyW',
    'lo7o2I9y7B0',
    'FK4jFyHTOn1',
    'rq17kE1q-sU',
    'j5sysMlEIRt',
    'UlaGX8O1u2t',
    'z71mh9j7kOV',
    '5287dywhIB_',
    'kDfRemvg7G7',
    '95l_Z5u828Y',
    'qdPO2WJJF2r',
    'hlb17pa5aXZ',
    'IChYy-Jzvxr',
    'VpD3ai8RiCA',
    'RQZdkB609G9',
    'PmHwgnej4cs',
    '_kqNxM-2DeI',
    '6uBrp81SmRq',
    'ABcSoXsfM2D',
    'nKEGAJN4oLg',
    'B12BgrNlxOi',
    'hFs3e0Rqm7l',
    'eZSH3F4Vcge',
    'Urph9OsoNNO',
    'dSBvUqH8j0I',
    'oZlZavISxeV',
    'pGycauNY1rj',
    'o93DTWND7pT',
    'Lwv1ZmELnLz',
    'cozkX9bvXSc',
    'WfqlP_8vowU',
    'SFhvbvJhOZH',
    'lheCMlWDubH',
    'ySBVdswtKnv',
    'wBshhvgGwHE',
    '84bAq00-At-',
    '1xQhIYhzFM3',
    'Hyknqp1GJ4g',
    'dqv5TDF0RCY',
    'GwsJbWGEGfs',
    '8ZN7GW8IORK',
    '6k9sw7jFojK',
    'TCEmpYNvm6A',
    'cnhUzklq3ZC',
    'O2kXD8HQjs9',
    'PhTTd8wsidJ',
    'lo-ys7VMtxE',
    'SogfMpN1ric',
    'YwusyECu1-B',
    'hh3W_k_xqAO',
    'JTeRDI0ApFG',
    'Kq-QtVqarTG',
    'TRyGNGSANou',
    '7qD7b9qxn5v',
    'ODoVqLATUJ4',
    '3YVf1ToaPIC',
    'YxZA1Douu6C',
    'rcvcAhq1OCm',
    'Z0oyO78laxz',
    '49CFjJ8wl21',
    'F-S1qbQdLMx',
    '8DcLoHtAQok',
    '2rgVO35vQ4O',
    'MCLCTmY1vnY',
    'nVr5X6KugxV',
    'BvgX5GOvXpD',
    'q_0lcgfJV3K',
    'ZMpwT07y4Jq',
    '8jPRfU__6k0',
    '7pD1iobsGzh',
    'XyWA_CGsdI8',
    'ZMvMNvwzhL9',
    'EPVpY_qtJMd',
    'QcwVpwDdVFC',
    'Gt4Q-iQmalo',
    'vjv0tRXjIuW',
    'xzr0rFHs3aJ',
    'EkAyyFPeVdl',
    '2o1qDJQnSlX',
    'uNoqEiQDDuq',
    'iOGo6SD575h',
    'phnXfQNkSI2',
    'X6uWAy3gPZo',
    'DdUHw0RmGIg',
    'Ox6M2YbZJdr',
    'ZKRUahRl9Ax',
    'LVnxt9hZk_7',
    '4o0DOYpwjyU',
    'w6LyOGrBnAE',
    'H07_7ZV_XWJ',
    'RkDEl0yYQGh',
    'JoIUmwEg7BV',
    'j7W0EgfkPLv',
    'M5sNla-cXgo',
    'NL8rQCp_zem',
    'tAgQSt3oE1J',
    'dAgTnUN1-n5',
    '18-8uK8wfzi',
    'v8BstLJTnL8',
    'kOKSScINq_l',
    'kMBAeBT9qiZ',
    'PPQIos_LQWf',
    '0WIaNkgO19j',
    'dJdNBuKqrJX',
    'u0NHCuyGLbj',
    'fWIgNDNNIT5',
    'CxWHcbp5kkT',
    'g_TlWcksRZl',
    'I07evcoHMx_',
    'D36vGYOsJH0',
    'YcT9v2G_CCD',
    'FUTFeKnD9vW',
    'FCr7oilSbl3',
    'Os4u-Fo_VD9',
    '2qncV5zFjko',
    '7iNDNKs2lYk',
    'u4Z40LwBLhU',
    'p-NX5d0kW4y',
    '_GOrkCG86lv',
    'x2-MUYKALJ8',
    '8ljtt7ecGe_',
    'ay5gVuGjZ4W',
    'bpvj_M-ecKH',
    'N9zmetbWUkY',
    'vVJCTP1DXMl',
    'EIve0M9Th57',
    'mkBs6PGuVxq',
    '_kEGUVfmToJ',
    'vCTxheSI_dK',
    'hlXCR-1vcjo',
    'NY77BlWEAXi',
    '5GCKqwM39c9',
    'hIAPXhrf6jG',
    'CeTNrlWTPo6',
    'PAbncIhyA_d',
    'oAlVPfHf3NJ',
    'G1xLUzfFJQN',
    'jiV0TDndrD_',
    '0zpaKpKBg8e',
    'spsg_UASmv_',
    '9UyYLBQZaTR',
    'Nmj6QDXVCmW',
    'KeoqNQ23yHH',
    'MwP0nGsQNpz',
    'ENefjBnmR7z',
    '7zox8q46m0z',
    'Q-3eqs2m576',
    'ONlUFMFJKj1',
    '_Ka5peXwLa_',
    'ZStTGJAYrgt',
    'euA9GIzWWLu',
    'eluFoEEHG3v',
    'sS_QmS0G9NA',
    '2TCXr605_3t',
    'QbJdqqmB2cU',
    'JemvYFfRtV-',
    'XBR9kvHd1RU',
    'xvXeFFXSdv2',
    'o1HdRQF1htG',
    '635ieNtllDm',
    'LqTrVAGxrZL',
    'z5Hz4x3qtos',
    'rhHKY3GeB4Z',
    'mIsPnNbOQLf',
    'LMKCcw-Kset',
    '4ViWIGEktz9',
    'mdYEWerRb3U',
    'VG5ViHQ0Pr3',
    'uCWIUeIP1YS',
    'sm01bO1WgTp',
    'SPuHws-1jqO',
    'y7YY_N8x8lu',
    'c-RBHc9CIxI',
    '9T9kr1DGNgT',
    'DFPOWPpnwky',
    'wSuIHErd-qE',
    'EAiQIAx0gw8',
    'axBzn2GjyG-',
    'gyS4lh25ZLt',
    'jCM-ynwrxOG',
    'xptGyEmG8LI',
    'aXKfyEYqJZ9',
    '40gmMDh5IwL',
    'sbFeCo0lTUC',
    '0-1bYgf5vIj',
    'pw9KrIzrxp_',
    'f0vm6_D69wL',
    'pdl2tf2gda1',
    '635ETMLW7uN',
    'EZWkt2ZIyY7',
    '4rFS4lLIw0v',
    '-pOHdUUdmZp',
    'YnEB7oqWREo',
    'nCalgvuihYA',
    'noNPM1gn-U6',
    'BJ3zZU7rIJI',
    'uR7wGLYUiIL',
    'pUdV4T9ZNoX',
    'I6iUNmJ4PlT',
    'Myc3ptxHT8c',
    'H0u6YkoIqsG',
    'xfyqWWEcxhq',
    'uL_jVSCk7Fw',
    'ncXFk3keVMJ',
    'fsOs5nDHHsF',
    '7fQ9Hshq2Rm3',
    'q7F06Y-0sW-9',
    'ybfekTDegAxd',
    'WYCQXjbwJiS8',
    'OeHIdNB5zZeP',
    'rCcSuPm3QKbh',
    'qXVFuO3offcO',
    'trup6EP-Xzv4',
    'j5oR9X9NnPu5',
    '0KvAOPILRrRk',
    'NDB0-toLU6ol',
    '44JOQgYqqUuw',
    'nrV_xgJPdi7V',
    'vjesJBpATAQl',
    '-pJ_YLCguIFJ',
    'gjkguFepinZa',
    'PbyZ4UWpen-s',
    'nuRNibAO5LAh',
    'um6ImwHXYdt_',
    '_JAibpDzTNUq',
    '168f5gva60Jx',
    'HxbTMqy9k0tZ',
    'HJcoOLitNCoG',
    'BQpHB9L4GieT',
    'pIZh5pmtK3BJ',
    'O6kr3KOQtMjn',
    'K7xDbt01jYwd',
    'R3pux2p1xpgR',
    'f6KjIB6CCbOr',
    'GnoEtc_Vz4p5',
    'HghdHDPhTFrd',
    'KpS0HKREhs4c',
    'Rex8oPLHgqzf',
    '6ChYlWXjoFlz',
    '5fmSoBbtGXzv',
    '3HaUdcrw7Gtc',
    'XOnXV9l3nPbw',
    'CklQhZ8tkjke',
    '35zRFwWitaEF',
    'K1Skux-i1YdI',
    'OrkgQIXPcMUG',
    'C21PVGAoXNSG',
    'CUCEbJkZIp0S',
    'v_e4n8m8iIii',
    'z9vCIcx1n0eW',
    'LmfewOB1B0pl',
    'EBKL02PRSM2D',
    'NWGOPxfYKzt8',
    'fNGW6p33KSuw',
    'X2-0VTwhW241',
    'ZKdeAbf7tcGj',
    'j6_v1GefIWHc',
    'Jz8-iZDWdoRF',
    '1-GwrA9YOrY_',
    'GekzNyxYeU_3',
    'fw6_zNziVCuM',
    '9g5ZW7Cne4Wa',
    'Dl1EflrqHBIQ',
    'bXjQUdY3XWPS',
    'LQdcFTOJqLTr',
    'uKsWY60liYLy',
    'LUHSqmvxGPZO',
    'KrvAZo_xdALO',
    'ygUDdP-x4YrX',
    '6u_Q0E6EcoSK',
    'mMHe8j9iFtfu',
    'kl24ryvi0YyT',
    'uBZzetbG7ln1',
    'G0qKicG5JRXK',
    'bFnOKuaFX1UD',
    '90ERx5_uhnmm',
    'UfievBzfLYU7',
    'z2a9i2dEo0RW',
    'arG67bOHtM5p',
    'iKJxTxPEQ3ow',
    'UOXEfNGOtPD0',
    'Lnqiud-YAE8P',
    '9J_aQDTTZGkM',
    'bje5zWVRrTx1',
    'EQ9ffAA_0CR-',
    '_r8AW7-9PueV',
    '5NlNv5hKNLEy',
    'ZcWYWFzlgpmj',
    '0VBEDmXLH4dd',
    'NvdGL0iAlMwS',
    'QRfWCkKXgbsZ',
    'sZKUiiuEaDry',
    'VKZ9tV6RWDql',
    'Jm9X2oF6eKzK',
    '45hJGec4v6Vn',
    'Ke6Buo_8Pxcu',
    's75AUDQDvXDx',
    'MUjuIxL5G3mC',
    'Jsk_0Y4g4UK_',
    'CBET8hOXjrKd',
    'aBzQRhYdntWd',
    'rRLDRRBJZNYc',
    'nv7bV4-IpQc0',
    'h-qsPqAu3yuX',
    '1HbHDH5MGVQm',
    'GttqpFxhOVb6',
    'NhXfKZxRozEE',
    'fENarzE0vKCw',
    'cjjuU5ziMkz6',
    '9qVxyGIpO8Dx',
    'Hut98VHT97Um',
    'K5O7rNkYaRoT',
    'Zdv8Cx4Ak-oZ',
    'vVFRBZ4qBiXn',
    'IhLfZy4IjRD8',
    'WbEt4PGs9g3t',
    'UzL6uYTyE63-',
    'P3CXcYlOE1iA',
    'ioedBfwV3p-T',
    'j0jLMqt3WHPH',
    'rYA3LX_9A_x3',
    'gBwc17SdR72b',
    'RY4wtKpVLgCZ',
    'Yj4w9zzpTW2e',
    'LUpbEuoOJ4fE',
    '9K_ASUPTxSGb',
    'kcU52ki5J9uI',
    'UNIEREjt0Gkh',
    'LQIJbHjjJtwa',
    'SJbVMYEhvh8i',
    'eKqA6mWyjvfv',
    'gOtwZi0ZV4Ku',
    'rKUEbltBYJ_l',
    'XDBOgZSKFoxC',
    '87TfwaICA-mp',
    'QABWyxDyyl56',
    'KNAddiOmqYhz',
    'NMUEU5dlCBko',
    'Gco9E0NLP6eO',
    'rCwbcHBjeoTw',
    'Y-DnvtpZvhsb',
    '-I9MBkEoDdWK',
    '9e7jL3zTemtA',
    'satpN9mI1nMd',
    '3Hg1NSCbhJbo',
    'XSnDxj0Qjr0v',
    'Oo85zy0xvAge',
    'ee5Cgq_kaH7c',
    'V637U4pQhGa9',
    'KqvAPKBWLIxa',
    '4LY06VPCMGGh',
    'W3QPR8cbCqh1',
    'WhOVmQW4isoR',
    'btPndbGICyMV',
    'pbIWp8V7eMEL',
    'zfTFYP-iQykW',
    '6JtzH2TlZ-d3',
    '-t97kMtIsxab',
    'XdvJYbu3Y7-n',
    'ihiQk6glYdfR',
    '76OcxKGI0U4L',
    'OuGychqBzIre',
    '2moR2p5AqJea',
    'RJY9lMea7ptF',
    'U4J2BaLoSlDA',
    'soKkaKm0oOuB',
    'q8gDGW0G2FtR',
    'gt9VK4d0iCTR',
    'l5-6rma9vTGy',
    '17aGtHnvUIi8',
    'eed9vvDTvcAg',
    'buk8MUf0CsWe',
    'mTXoJv_Yo7N2',
    'f30-z1PTEUS6',
    'rLjwp1tCqm3m',
    '9fNtOxU82Hdy',
    'U8L4pYOwRrXc',
    'leDyn7sAvo0G',
    '4Y0n3H1PP0J6',
    'Dcg_IofyuqAL',
    'TroY4p30-Rhs',
    'CAYSItA2DU97',
    'CBNWIuOIDBCB',
    'NQNXokt5i_EQ',
    'NdZzYEVUqywE',
    'g7A4ur8wPUYT',
    '_KQEWM6lNXqW',
    'cU1rQ6U2WQTy',
    'yBKyD1Wu_uw8',
    '2j8k0LVbOWwZ',
    'BvqahOq94yFB',
    '8o06760SxuhE',
    'nRMUU2WCoGQj',
    'mypXEViY2dM9',
    'eoI05-tv0Qd1',
    'sVSxSGCPq5GA',
    'wTKaSqohSr6l',
    '1bUbSRDs6aLj',
    'V9IGFkl6WoIJ',
    'LdQYszp07s4s',
    'EfoWXL08Fku2',
    '9B_z2GPVr-sa',
    'j88u2WGK4RZs',
    'PRy1Nv8aSWst',
    'Sfm1XYIoBcI-',
    'I7AhXeWJIWYs',
    'ywO2adstz7Ma',
    'dV2jsMFNo1Nm',
    'lC2GdGdigZw2',
    'KkNDjQHDbyJn',
    'rYQoynJR4MqZ',
    'C7kli55rGf5g',
    'excenYge50Pz',
    'uZb5CsaAxxJL',
    'AhDX4tbCKpC8',
    'LLobt5uCx_aM',
    'Pc7tzClbRKuK',
    'IdXBWOofMZow',
    'zftcn99ppXLZ',
    'yOHWUFZ6R8oW',
    'fA-zBItKg5Yw',
    'rQ_XniMjUOfS',
    'RPhzLY0lRDmH',
    '1Nj9OvRoPyIT',
    'sUrZnAJOGwPc',
    'ku7i62n4iMyV',
    'YrUXwZcr0BZr',
    'kZVOh7eH30Rb',
    'oW-tgQ2pFb2R',
    'JYCVphGftd-I',
    'MTK7j37yPisO',
    'Jo5BW9KFm98P',
    '7GaUteKZihqQ',
    'gUD3frpTMajK',
    'S1mo-lqCl6XO',
    'krdgP6x-LzhE',
    'xHLiPT88SCF5',
    'w28CxY3GAHaa',
    'Lor2M33Ib6n_',
    'FzhPkhjA4LML',
    'FgbPHgRXFAzv',
    'GlgzgYUQGcaX',
    'ZGq2RJwPLIYf',
    'IgwhSuYNTzD9',
    'BkW6XEKF6iRX',
    'CClz8Y_V3F7w',
    'ijWqFcysVBMW',
    'hgCvc3f9vbB6',
    'TWRYRe8kZDl-'
];

const context = 'auction';
const aggregate = 'salesChannelInstanceVehicle';

let eventstoreInstance;

const sleep = function(timeout) {
    return new Promise((resolve) => {
        setTimeout(resolve, timeout);
    })
}

zbench('sharding', (z) => {
    z.setupOnce(async (done, b) => {
        debug(`setting up benchmarking for ${shardCount} shards`);
        await compose.upAll({
            cwd: path.join(__dirname),
            composeOptions: [["--profile", configs.profileMap[shardCount]]],
            callback: (chunk) => {
                debug('compose in progress: ', chunk.toString())
            }
        });

        const retryInterval = 1000;
        const connectCounterThreshold = 60;

        const promises = [];
        for (let i = 0; i < shardCount; i++) {
            const mysqlCheckPromise = new Promise(async (resolve, reject) => {
                let isConnected = false;
                let connectCounter = 0;
                while (connectCounter < connectCounterThreshold) {
                    try {
                        const mysqlConnection = await mysql2.createConnection({
                            host: 'localhost',
                            port: configs.clusterPorts[i],
                            user: 'root',
                            password: 'root'
                        });
                        await mysqlConnection.connect();
                        await mysqlConnection.query('CREATE DATABASE IF NOT EXISTS eventstore;');
                        await mysqlConnection.end();
                        isConnected = true;
                        debug(`connection successful to db ${i + 1}`);
                        break;
                    } catch (err) {
                        debug(`cannot connect to mysql for db ${i + 1}. sleeping for ${retryInterval}ms`);
                        connectCounter++;
                        await sleep(retryInterval);
                    }
                }
                if (isConnected) {
                    resolve();
                } else {
                    console.log('COULD NOT CONNECT TO MYSQL');
                    reject();
                }
            });
            promises.push(mysqlCheckPromise);
        }

        try {
            await Promise.all(promises);
        } catch (err) {
            console.log(err);
        }
        debug('connected to mysql setup complete');

        done();
    });

    z.teardownOnce(async (done, b) => {
        debug('docker compose down started');
        // await compose.down({
        //     cwd: path.join(__dirname)
        // })
        debug('docker compose down finished');
        done();
    });

    z.setup('sharding', async (done, b) => {
        const es = require('../lib/eventstore-projections/clustered-eventstore')(config);
        bluebird.promisifyAll(es);
        await es.initAsync();
        eventstoreInstance = es;
        done();
        debug('setup complete');
    });

    z.test('sharding', (b) => {
        const randomIndex = Math.floor((Math.random() * testAggregateIds.length));
        const eventPayload = {
            vehicleId: '',
            salesChannelInstanceVehicleId: '',
            previousLeader: null,
            isReserveMet: false,
            leader: {
                dealerFirstName: 'Test First ',
                dealerPhotoPath: 'test.com/profile-pic.jpg',
                dealerName: 'Test First Last',
                dealershipName: 'Test Dealership',
                dealerLastName: 'Last',
                bid: 145600,
                dealerId: 'dealer-id',
                dealershipId: 'dealership-id',
                bidType: 'regularbid'
            },
            bidders: null,
            extensionEndsAt: 1645064128300
        };

        // const aggregateId = shortid.generate(); // testAggregateIds[randomIndex];
        const aggregateId = testAggregateIds[randomIndex];

        eventPayload.salesChannelInstanceVehicleId = aggregateId;
        eventPayload.vehicleId = `vehicle-${aggregateId}`;
        
        b.start();
        eventstoreInstance.getFromSnapshot({
            aggregateId: aggregateId,
            aggregate: aggregate,
            context: context,
        }, (err, snapshot, stream) => {
            if (err) {
                b.error(err);
            } else {
                stream.addEvent({
                    aggregateId: aggregateId,
                    aggregate: aggregate,
                    context: context,
                    payload: eventPayload
                });
                stream.commit(() => {
                    b.end();
                });
            }
            
        });
    });

    z.teardown('sharding', (done) => {
        done();
    })
})