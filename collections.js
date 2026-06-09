/**
 * MELONI — KOLLEKTIONSKONFIGURATION
 * 
 * De 7 aktiva kollektionerna. Uppdatera shopify_id när
 * du skapat kollektionerna i Shopify-admin.
 * 
 * Så här hittar du shopify_id:
 * Shopify Admin → Products → Collections → klicka kollektionen
 * → URL:en visar: /collections/123456789 → det är ditt ID
 */

const COLLECTIONS = [
  {
    name:       'Fitness & Hälsa',
    handle:     'fitness-halsa',
    shopify_id: null,           // ← fyll i efter skapandet i Shopify
    emoji:      '💪',
    tags:       ['fitness', 'halsa', 'traning', 'sport', 'wellness'],
    cj_keywords: ['fitness', 'gym', 'yoga', 'sport', 'health', 'exercise'],
  },
  {
    name:       'Hem & Inredning',
    handle:     'hem-inredning',
    shopify_id: null,
    emoji:      '🏠',
    tags:       ['hem', 'inredning', 'kök', 'badrum', 'förvaring'],
    cj_keywords: ['home decor', 'kitchen', 'storage', 'bedroom', 'living room'],
  },
  {
    name:       'Tech & Gadgets',
    handle:     'tech-gadgets',
    shopify_id: null,
    emoji:      '🔌',
    tags:       ['tech', 'gadgets', 'elektronik', 'tillbehör'],
    cj_keywords: ['gadget', 'electronic', 'USB', 'wireless', 'smart', 'cable'],
  },
  {
    name:       'Skönhet & Välmående',
    handle:     'skonhet-valmande',
    shopify_id: null,
    emoji:      '✨',
    tags:       ['skönhet', 'välmående', 'hudvård', 'massage'],
    cj_keywords: ['beauty', 'skin care', 'massage', 'hair', 'cosmetic', 'wellness'],
  },
  {
    name:       'Mode & Accessoarer',
    handle:     'mode-accessoarer',
    shopify_id: null,
    emoji:      '👜',
    tags:       ['mode', 'accessoarer', 'väskor', 'smycken', 'kläder'],
    cj_keywords: ['fashion', 'bag', 'jewelry', 'accessories', 'watch', 'bracelet'],
  },
  {
    name:       'Resor & Outdoor',
    handle:     'resor-outdoor',
    shopify_id: null,
    emoji:      '🎒',
    tags:       ['resor', 'outdoor', 'camping', 'vandring', 'resväska'],
    cj_keywords: ['travel', 'outdoor', 'camping', 'hiking', 'backpack', 'luggage'],
  },
  {
    name:       'Livsstil',
    handle:     'livsstil',
    shopify_id: null,
    emoji:      '🌿',
    tags:       ['livsstil', 'hobby', 'fritid', 'presenter', 'övrigt'],
    cj_keywords: ['lifestyle', 'gift', 'hobby', 'leisure', 'organizer', 'daily'],
  },
];

/**
 * Matcha en produkt mot rätt kollektion baserat på titel + taggar.
 * Returnerar array av matching collection handles.
 * 
 * @param {string} title       Produkttitel
 * @param {string[]} tags      Produkttaggar från Mercury/AI
 * @returns {string[]}         Matchande collection handles
 */
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

  // Fallback: om inget matchar → Livsstil
  if (matches.length === 0) matches.push('livsstil');

  return matches;
}

/**
 * Hämta Shopify collection ID från handle.
 * Returnerar null om inte konfigurerat än.
 */
function getCollectionId(handle) {
  const col = COLLECTIONS.find(c => c.handle === handle);
  return col ? col.shopify_id : null;
}

module.exports = { COLLECTIONS, matchCollections, getCollectionId };
