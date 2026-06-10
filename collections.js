/**
 * MELONI — KOLLEKTIONSKONFIGURATION
 * Fitness / Outdoor / Health niche
 */

const COLLECTIONS = [
  {
    name:        'Träning & Fitness',
    handle:      'traning-fitness',
    shopify_id:  null,
    emoji:       '💪',
    tags:        ['träning','fitness','gym','yoga','crossfit','styrka','kondition'],
    cj_keywords: ['fitness','gym','workout','yoga','resistance','training','exercise','crossfit'],
  },
  {
    name:        'Friluftsliv & Outdoor',
    handle:      'friluftsliv-outdoor',
    shopify_id:  null,
    emoji:       '🏔️',
    tags:        ['outdoor','vandring','camping','cykling','trail','klättring','löpning'],
    cj_keywords: ['hiking','outdoor','camping','cycling','trail','trekking','climbing','running'],
  },
  {
    name:        'Återhämtning & Hälsa',
    handle:      'aterhamtning-halsa',
    shopify_id:  null,
    emoji:       '🧘',
    tags:        ['återhämtning','massage','kompression','hållning','stretching','sömn'],
    cj_keywords: ['recovery','massage','brace','compression','posture','stretching','sleep','foam roller'],
  },
  {
    name:        'Smart Teknik',
    handle:      'smart-teknik',
    shopify_id:  null,
    emoji:       '⌚',
    tags:        ['smartwatch','gps','tracker','hörlurar','kamera','puls'],
    cj_keywords: ['smartwatch','gps watch','tracker','earbuds','headphones','action camera','heart rate'],
  },
  {
    name:        'Kost & Vätska',
    handle:      'kost-vatska',
    shopify_id:  null,
    emoji:       '💧',
    tags:        ['vattenflaska','protein','kosttillskott','shaker','hydrering','meal prep'],
    cj_keywords: ['water bottle','protein','supplement','shaker','hydration','electrolyte','meal prep'],
  },
  {
    name:        'Utrustning & Tillbehör',
    handle:      'utrustning-tillbehor',
    shopify_id:  null,
    emoji:       '🎒',
    tags:        ['väska','ryggsäck','handskar','bälte','matta','rep','utrustning'],
    cj_keywords: ['gym bag','backpack','gloves','belt','mat','rope','equipment','accessories'],
  },
  {
    name:        'Livsstil',
    handle:      'livsstil',
    shopify_id:  681614344529,
    emoji:       '🌿',
    tags:        ['livsstil','aktiv','vardagsmotion','hälsosam'],
    cj_keywords: ['lifestyle','active','daily','wellness','healthy'],
  },
];

function matchCollections(title, tags = []) {
  const titleLower = title.toLowerCase();
  const tagsLower  = tags.map(t => t.toLowerCase());
  const matches    = [];

  for (const col of COLLECTIONS) {
    const hit = col.cj_keywords.some(kw =>
      titleLower.includes(kw.toLowerCase()) ||
      tagsLower.some(t => t.includes(kw.toLowerCase()))
    );
    if (hit) matches.push(col.handle);
  }

  if (matches.length === 0) matches.push('traning-fitness');
  return matches;
}

function getCollectionId(handle) {
  const col = COLLECTIONS.find(c => c.handle === handle);
  return col ? col.shopify_id : null;
}

module.exports = { COLLECTIONS, matchCollections, getCollectionId };
