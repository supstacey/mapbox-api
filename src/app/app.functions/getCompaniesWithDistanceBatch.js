// For HubSpot API calls
const hubspot = require('@hubspot/api-client');
// For obtaining geocoding data
const MapboxClient = require('@mapbox/mapbox-sdk/services/geocoding');
// For calculating the distance between two points
const turf = require('@turf/turf');

// To fetch needed company properties
const PROPERTIES_TO_FETCH = [
  'hs_object_id',
  'city',
  'state',
  'address',
  'domain',
  'phone',
  'name',
  'annualrevenue',
  'branch_location',
  'hubspot_owner_id',
];

// Function to fetch owner details from HubSpot
async function getOwnerDetails(ownerIds, hubspotClient) {
  // Fetch owner details from HubSpot
  const ownerResponse = await hubspotClient.crm.owners.basicApi.getBatchById(ownerIds);

  // Map owner IDs to owner names
  const ownerMap = ownerResponse.results.reduce((map, owner) => {
    map[owner.id] = owner.firstName + ' ' + owner.lastName; // Adjust based on the available fields
    return map;
  }, {});

  return ownerMap;
}

// Entry function of this module, it fetches batch of companies and calculates distance to the current company record
exports.main = async (context = {}) => {
  let currentCompany = await extendWithGeoCoordinates(context.propertiesToSend);
  if (!currentCompany.coordinates) {
    throw new Error(
      'Unable to calculate geo coordinates. Please specify an address for the record.'
    );
  }

  const { batchSize } = context.event.payload;
  const hubspotClient = new hubspot.Client({
    accessToken: process.env.hubspot_access_token, // Replace with your HubSpot API access token
  });

  let otherCompanies = await getOtherCompaniesBatch({
    hubspotClient,
    batchSize,
    currentCompany,
  });

  // Extend companies records with geo coordinates and distance to the current company
  otherCompanies = await Promise.all(
    otherCompanies.map((company) => extendWithGeoCoordinates(company))
  );
  otherCompanies = await extendWithDistance({
    coordinatesFrom: currentCompany.coordinates,
    companies: otherCompanies.filter(({ coordinates }) => !!coordinates),
  });

  // Get unique owner IDs from companies
  const ownerIds = [...new Set(otherCompanies.map(company => company.hubspot_owner_id).filter(id => id))];
  const ownerDetails = await getOwnerDetails(ownerIds, hubspotClient);

  // Add owner names to company data
  const companiesWithOwnerNames = otherCompanies.map(company => ({
    ...company,
    ownerName: ownerDetails[company.hubspot_owner_id] || 'Unknown',
  }));

  return {
    companies: companiesWithOwnerNames,
  };
};

// Function to fetch companies batch using HubSpot API client
async function getOtherCompaniesBatch({
  hubspotClient,
  batchSize,
  currentCompany,
}) {
  const apiResponse = await hubspotClient.crm.companies.basicApi.getPage(
    batchSize,
    undefined,
    PROPERTIES_TO_FETCH
  );

  return apiResponse.results
    .map((company) => ({
      ...company,
      ...company.properties,
    }))
    .filter(
      // Exclude current company record from list
      ({ hs_object_id }) => hs_object_id != currentCompany.hs_object_id
    );
}

// Function to query geo coordinates based on company address
async function extendWithGeoCoordinates(company) {
  try {
    return {
      ...company,
      coordinates: await getGeoCoordinates({
        address: buildFullAddress(company),
      }),
    };
  } catch (e) {
    return company;
  }
}

// Function to calculate the distance from current company record
async function extendWithDistance({ coordinatesFrom, companies }) {
  return Promise.all(
    companies.map(async (company) => {
      const distance = getDistance({
        coordinatesFrom,
        coordinatesTo: company.coordinates,
      });
      // Return existing company properties together with calculated distance
      return {
        ...company,
        distance,
      };
    })
  );
}

const buildFullAddress = ({ city, state, address }) => {
  return `${city} ${state} ${address}`;
};

// Function to obtain geographic coordinates for specified address
async function getGeoCoordinates({ address }) {
  // Use Mapbox Geocoding API
  const mapboxClient = MapboxClient({
    accessToken: process.env.mapbox,
  });
  const response = await mapboxClient.forwardGeocode({ query: address }).send();

  return response.body.features[0].geometry.coordinates;
}

// Function to calculate the distance between 2 points
function getDistance({ coordinatesFrom, coordinatesTo }) {
  return turf.distance(turf.point(coordinatesFrom), turf.point(coordinatesTo), {
    units: 'miles',
  });
}
