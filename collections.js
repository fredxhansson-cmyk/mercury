/**
 * MELONI — KOLLEKTIONSKONFIGURATION
 * Fitness / Outdoor / Health niche — 55 kollektioner
 */

const COLLECTIONS = [
  // ── HERR ──────────────────────────────────────────────────
  { name: 'Herr', handle: 'herr', shopify_id: 681677783377, tags: ['herr'], cj_keywords: ['men sport','mens athletic','male fitness'] },
  { name: 'Herr T-shirts och Linnen', handle: 'herr-t-shirts', shopify_id: 681677881681, tags: ['herr','t-shirt'], cj_keywords: ['men t-shirt','men tank top','men sport tee','men gym shirt'] },
  { name: 'Herr Linnen', handle: 'herr-linnen', shopify_id: 681677914449, tags: ['herr','linne'], cj_keywords: ['men tank','men vest','men sleeveless'] },
  { name: 'Herr Hoodies och Sweatshirts', handle: 'herr-hoodies', shopify_id: 681677979985, tags: ['herr','hoodie'], cj_keywords: ['men hoodie','men sweatshirt','men pullover'] },
  { name: 'Herr Funktionskläder', handle: 'herr-funktionskl-der', shopify_id: 681678012753, tags: ['herr','funktionskläder'], cj_keywords: ['men compression shirt','men base layer','men thermal shirt','men running shirt'] },
  { name: 'Herr Kompressionskläder', handle: 'herr-kompressionskl-der', shopify_id: 681678045521, tags: ['herr','kompression'], cj_keywords: ['men compression tights','men compression pants','men compression leggings'] },
  { name: 'Herr Shorts', handle: 'herr-shorts', shopify_id: 681678078289, tags: ['herr','shorts'], cj_keywords: ['men gym shorts','men running shorts','men training shorts','men sport shorts'] },
  { name: 'Herr Byxor och Joggers', handle: 'herr-byxor', shopify_id: 681678111057, tags: ['herr','byxor'], cj_keywords: ['men joggers','men sweatpants','men track pants','men training pants'] },
  { name: 'Herr Underställ', handle: 'herr-underst-ll', shopify_id: 681678176593, tags: ['herr','underställ'], cj_keywords: ['men thermal underwear','men base layer bottom','men long johns'] },
  { name: 'Herr Jackor', handle: 'herr-jackor', shopify_id: 681678143825, tags: ['herr','jacka'], cj_keywords: ['men sport jacket','men windbreaker','men rain jacket','men softshell','men fleece jacket'] },
  { name: 'Herr Strumpor och Underkläder', handle: 'herr-strumpor', shopify_id: 681678242129, tags: ['herr','strumpor'], cj_keywords: ['men sport socks','men running socks','men athletic socks'] },
  { name: 'Herr Träningsskor', handle: 'herr-traningsskor', shopify_id: 681678340433, tags: ['herr','träningsskor'], cj_keywords: ['men training shoes','men gym shoes','men crossfit shoes','men workout shoes'] },
  { name: 'Herr Löparskor', handle: 'herr-loparskor', shopify_id: 681678307665, tags: ['herr','löparskor'], cj_keywords: ['men running shoes','men jogging shoes'] },
  { name: 'Herr Trailskor', handle: 'herr-trailskor', shopify_id: 681678373201, tags: ['herr','trailskor'], cj_keywords: ['men trail shoes','men trail running shoes'] },
  { name: 'Herr Vandringskängor', handle: 'herr-vandring', shopify_id: 681678438737, tags: ['herr','vandring'], cj_keywords: ['men hiking boots','men trekking boots'] },
  { name: 'Herr Sandaler', handle: 'herr-sandaler', shopify_id: 681678471505, tags: ['herr','sandaler'], cj_keywords: ['men sport sandals','men outdoor sandals'] },

  // ── DAM ───────────────────────────────────────────────────
  { name: 'Dam', handle: 'dam', shopify_id: 681677816145, tags: ['dam'], cj_keywords: ['women sport','womens athletic','female fitness'] },
  { name: 'Dam T-shirts och Linnen', handle: 'dam-t-shirts', shopify_id: 681678504273, tags: ['dam','t-shirt'], cj_keywords: ['women t-shirt','women tank top','women sport top','women gym shirt'] },
  { name: 'Dam Hoodies och Sweatshirts', handle: 'dam-hoodies', shopify_id: 681678569809, tags: ['dam','hoodie'], cj_keywords: ['women hoodie','women sweatshirt'] },
  { name: 'Dam Funktionskläder', handle: 'dam-funktionskl-der', shopify_id: 681678602577, tags: ['dam','funktionskläder'], cj_keywords: ['women compression shirt','women base layer','women thermal shirt','women running shirt'] },
  { name: 'Dam Kompressionskläder', handle: 'dam-kompressionskl-der', shopify_id: 681678635345, tags: ['dam','kompression'], cj_keywords: ['women compression tights','yoga pants','women leggings sport'] },
  { name: 'Dam Shorts och Tights', handle: 'dam-shorts', shopify_id: 681678668113, tags: ['dam','shorts'], cj_keywords: ['women gym shorts','women running shorts','women biker shorts'] },
  { name: 'Dam Byxor och Leggings', handle: 'dam-byxor', shopify_id: 681678700881, tags: ['dam','leggings'], cj_keywords: ['women joggers','women training pants','women sport leggings'] },
  { name: 'Dam Jackor', handle: 'dam-jackor', shopify_id: 681678733649, tags: ['dam','jacka'], cj_keywords: ['women sport jacket','women windbreaker','women rain jacket','women fleece'] },
  { name: 'Dam Underställ', handle: 'dam-underst-ll', shopify_id: 681678766417, tags: ['dam','underställ'], cj_keywords: ['women thermal underwear','women base layer'] },
  { name: 'Sport-BH Lätt Support', handle: 'sport-bh-latt', shopify_id: 681678799185, tags: ['dam','sport-bh'], cj_keywords: ['sports bra light','yoga bra','low impact bra'] },
  { name: 'Sport-BH Medium Support', handle: 'sport-bh-medium', shopify_id: 681678831953, tags: ['dam','sport-bh'], cj_keywords: ['sports bra medium','medium support bra'] },
  { name: 'Sport-BH Hög Support', handle: 'sport-bh-hog', shopify_id: 681678864721, tags: ['dam','sport-bh'], cj_keywords: ['sports bra high','high impact bra','running bra'] },
  { name: 'Dam Löparskor', handle: 'dam-loparskor', shopify_id: 681678897489, tags: ['dam','löparskor'], cj_keywords: ['women running shoes','women jogging shoes'] },
  { name: 'Dam Träningsskor', handle: 'dam-traningsskor', shopify_id: 681678930257, tags: ['dam','träningsskor'], cj_keywords: ['women training shoes','women gym shoes','women crossfit shoes'] },
  { name: 'Dam Vandringskängor', handle: 'dam-vandring', shopify_id: 681678963025, tags: ['dam','vandring'], cj_keywords: ['women hiking boots','women trekking boots'] },

  // ── BARN ──────────────────────────────────────────────────
  { name: 'Barn', handle: 'barn', shopify_id: 681677848913, tags: ['barn'], cj_keywords: ['kids sport','children athletic','youth fitness'] },
  { name: 'Barn T-shirts och Hoodies', handle: 'barn-t-shirts', shopify_id: 681678995793, tags: ['barn','t-shirt'], cj_keywords: ['kids t-shirt','kids hoodie','children sport shirt'] },
  { name: 'Barn Shorts och Byxor', handle: 'barn-shorts', shopify_id: 681679028561, tags: ['barn','shorts'], cj_keywords: ['kids shorts','children shorts','kids sport pants'] },
  { name: 'Barn Jackor och Regnkläder', handle: 'barn-jackor', shopify_id: 681679061329, tags: ['barn','jacka'], cj_keywords: ['kids jacket','children rain jacket'] },
  { name: 'Barn Outdoorkläder', handle: 'barn-outdoor', shopify_id: 681679094097, tags: ['barn','outdoor'], cj_keywords: ['kids outdoor clothes','children hiking clothes'] },
  { name: 'Barn Underställ', handle: 'barn-underst-ll', shopify_id: 681679126865, tags: ['barn','underställ'], cj_keywords: ['kids thermal','children base layer'] },
  { name: 'Barn Träningsskor', handle: 'barn-traningsskor', shopify_id: 681679192401, tags: ['barn','träningsskor'], cj_keywords: ['kids training shoes','children gym shoes'] },
  { name: 'Barn Löparskor', handle: 'barn-loparskor', shopify_id: 681679159633, tags: ['barn','löparskor'], cj_keywords: ['kids running shoes','children running shoes'] },
  { name: 'Barn Outdoorskor', handle: 'barn-outdoorskor', shopify_id: 681679225169, tags: ['barn','skor'], cj_keywords: ['kids outdoor shoes','children hiking shoes'] },
  { name: 'Barn Vandringskängor', handle: 'barn-vandring', shopify_id: 681679257937, tags: ['barn','vandring'], cj_keywords: ['kids hiking boots','children trekking'] },

  // ── AKTIVITET & KATEGORI ───────────────────────────────────
  { name: 'Träning & Fitness', handle: 'traning-fitness', shopify_id: 681667985745, tags: ['träning','fitness'], cj_keywords: ['resistance bands','ab roller','pull up bar','jump rope','gym equipment','workout equipment','fitness equipment','exercise equipment'] },
  { name: 'Friluftsliv & Outdoor', handle: 'friluftsliv-outdoor', shopify_id: 681668018513, tags: ['outdoor','friluftsliv'], cj_keywords: ['outdoor gear','camping gear','hiking gear','trekking poles','headlamp','carabiner','sleeping bag'] },
  { name: 'Återhämtning & Hälsa', handle: 'aterhamtning-halsa', shopify_id: 681668051281, tags: ['återhämtning','hälsa'], cj_keywords: ['massage gun','foam roller','knee brace','elbow brace','ankle support','posture corrector','acupressure mat','recovery'] },
  { name: 'Smart Teknik', handle: 'smart-teknik', shopify_id: 681668084049, tags: ['teknik','smartwatch'], cj_keywords: ['smartwatch','gps watch','fitness tracker','sport earbuds','wireless earphones sport','action camera','cycling computer','heart rate monitor'] },
  { name: 'Kost & Vätska', handle: 'kost-vatska', shopify_id: 681668116817, tags: ['kost','vätska'], cj_keywords: ['water bottle','protein shaker','hydration vest','electrolyte','meal prep container','supplement organizer','blender bottle'] },
  { name: 'Utrustning & Tillbehör', handle: 'utrustning-tillbehor', shopify_id: 681668149585, tags: ['utrustning'], cj_keywords: ['gym bag','sport backpack','lifting gloves','weightlifting belt','gym mat','sport towel','resistance loop'] },
  { name: 'Löpning', handle: 'lopning', shopify_id: 681679290705, tags: ['löpning'], cj_keywords: ['running belt','running vest','running headband','arm band phone','reflective running'] },
  { name: 'Vandring och Camping', handle: 'vandring', shopify_id: 681679389009, tags: ['vandring','camping'], cj_keywords: ['hiking backpack','camping tent','trekking poles','sleeping pad','camp light','water filter'] },
  { name: 'Cykling', handle: 'cykling', shopify_id: 681679356241, tags: ['cykling'], cj_keywords: ['cycling gloves','bike light','cycling helmet','bike bag','cycling shorts','bike computer'] },
  { name: 'Yoga', handle: 'yoga', shopify_id: 681679323473, tags: ['yoga'], cj_keywords: ['yoga mat','yoga block','yoga strap','yoga wheel','pilates ring','meditation cushion'] },
  { name: 'Bästsäljare', handle: 'bestsellers', shopify_id: 681677750609, tags: ['bästsäljare'], cj_keywords: [] },
  { name: 'Livsstil', handle: 'livsstil', shopify_id: 681614344529, tags: ['livsstil'], cj_keywords: ['active lifestyle','wellness','healthy living','sport lifestyle'] },
];

function matchCollections(title, tags = []) {
  const titleLower = title.toLowerCase();
  const tagsLower  = tags.map(t => t.toLowerCase());
  const matches    = new Set();

  for (const col of COLLECTIONS) {
    if (!col.cj_keywords.length) continue;
    const hit = col.cj_keywords.some(kw =>
      titleLower.includes(kw.toLowerCase()) ||
      tagsLower.some(t => t.includes(kw.toLowerCase()))
    );
    if (hit) matches.add(col.handle);
  }

  // Gender detection
  const isMen = /\bmen\b|\bmale\b|\bherr\b/i.test(title) || tagsLower.includes('herr');
  const isWomen = /\bwomen\b|\bfemale\b|\bladies\b|\bdam\b/i.test(title) || tagsLower.includes('dam');
  const isKids = /\bkid\b|\bchild\b|\bboys?\b|\bgirls?\b|\byouth\b|\bbarn\b/i.test(title) || tagsLower.includes('barn');

  if (isMen) matches.add('herr');
  if (isWomen) matches.add('dam');
  if (isKids) matches.add('barn');

  // Fallback
  if (matches.size === 0) matches.add('traning-fitness');

  return [...matches];
}

function getCollectionId(handle) {
  const col = COLLECTIONS.find(c => c.handle === handle);
  return col ? col.shopify_id : null;
}

module.exports = { COLLECTIONS, matchCollections, getCollectionId };
