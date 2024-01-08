from bs4 import BeautifulSoup


class SoupContentParser(object):

    @classmethod
    def get_title(cls, product):
        try:
            title = product.find('h2', {'class': 'a-size-mini'}).get_text(strip=True)
            return title
        except Exception:
            try:
                title = product.find('span', {'id': 'productTitle'}).get_text(strip=True)
                return title
            except:
                return None

    @classmethod
    def get_price(cls, product):
        try:
            price = product.find('span', {'class': 'a-price'}).find('span', {'class': 'a-offscreen'}).get_text(
                strip=True)
            return price
        except Exception:
            return None

    @classmethod
    def get_brand(cls, product):
        try:
            brand = product.find('div', {'id': 'bylineInfo_feature_div'}).get_text(strip=True)
            return brand
        except Exception:
            return None

    @classmethod
    def get_rating(cls, product):
        try:
            rating = product.find('i', {'class': 'a-icon-star-small'}).get_text(strip=True)
            return float(rating.split(' ')[0])
        except Exception:
            try:
                rating = product.find('span', {'class': 'a-size-base a-color-base'}).get_text(strip=True)
                return float(rating.split(' ')[0])
            except Exception:
                return None

    @classmethod
    def amount_reviws(cls, product):
        try:
            amount_of_reviews = int(product.find("span", {"id": "acrCustomerReviewText"}).text.split(' ')[0])
            return amount_of_reviews
        except Exception as exc:
            return None

    @classmethod
    def get_reviews(cls, product):
        try:
            reviews = product.find('span', {'class': 'a-size-base'}).get_text(strip=True)
            return int(reviews)
        except Exception:
            return None

    @classmethod
    def get_sales_volume(cls, product):
        try:
            sales_dict = {"K": 1000, "k": 1000, "b": 1000000, "B": 1000000}
            sales_volume = None
            for item in product.findAll('span', {'class': 'a-size-base a-color-secondary'}):
                if '+ bought' in item.get_text(strip=True):
                    item = item.get_text(strip=True).split('+ bought')[0]
                    for k, v in sales_dict.items():
                        if k in item:
                            sales_volume = int(item.split(k)[0]) * v
                            break
                        else:
                            sales_volume = int(item)
            return sales_volume
        except Exception:
            return None

    @classmethod
    def get_sales_volume_of_good(cls, product):
        try:
            sales_dict = {"K": 1000, "k": 1000, "b": 1000000, "B": 1000000}
            sales_volume = None
            for item in product.findAll('span', {'id': '"social-proofing-faceout-title-tk_bought"'}):
                if '+ bought' in item.get_text(strip=True):
                    item = item.get_text(strip=True).split('+ bought')[0]
                    for k, v in sales_dict.items():
                        if k in item:
                            sales_volume = int(item.split(k)[0]) * v
                            break
                        else:
                            sales_volume = int(item)
            return sales_volume
        except Exception:
            return None

    @classmethod
    def get_website(cls, product):
        try:
            link = product.find('a', {'class': 'a-link-normal'})['href']
            return link
        except Exception:
            return None

    @classmethod
    def get_image_link(cls, product):
        try:
            image_link = product.find('img', {'class': 's-image'})['src']
            return image_link
        except Exception:
            return None

    @classmethod
    def get_image_link_good(cls, product):
        try:
            image_link = product.find('div', {'id': 'imgTagWrapperId'}).find('img')['src']
            return image_link
        except Exception:
            return None

    @classmethod
    def get_color(cls, product):
        try:
            color_variants_count = len(product.find_all('div', {'data-csa-c-type': 'link'}))
            return color_variants_count
        except Exception:
            return None

    @classmethod
    def get_colors_goods(cls, product):
        try:
            color_variants_count = len(product.find('ul', {
                'class': 'a-unordered-list a-nostyle a-button-list a-declarative a-button-toggle-group a-horizontal a-spacing-top-micro swatches swatchesRectangle imageSwatches'}).find_all(
                'li'))
            return color_variants_count
        except Exception:
            return None

    @classmethod
    def get_asin(cls, product):
        try:
            asin = product['data-asin']
            return asin
        except Exception:
            return None

    @classmethod
    def get_details(cls, product):
        main_details = {}
        try:
            for item in product.find('div', {'class': 'a-expander-partial-collapse-content'}).findAll('div', {
                'class': 'product-facts-detail'}):
                main_details[item.find('div', {'class': 'a-col-left'}).get_text(strip=True)] = item.find('div', {
                    'class': 'a-col-right'}).get_text(strip=True)
        except AttributeError as exc:
            if "'NoneType' object has no attribute 'findAll'" in str(exc):
                pass
            else:
                raise exc
        main_details['About this item'] = []
        try:
            for elem in product.find('div', {'class': 'a-expander-partial-collapse-content'}).findAll('ul', {
                'class': 'a-spacing-small'}):
                main_details['About this item'].append(elem.text.strip())
        except:
            pass
        try:
            main_details['Product Description'] = product.find('div', {'id': 'productDescription'}).text.strip()
        except:
            pass

        try:
            product_details = product.find('div', {'id': 'detailBullets_feature_div'}).findAll('span',
                                                                                               {'class': 'a-list-item'})
        except AttributeError:
            product_details = product.find('div', {'id': 'prodDetails'}).findAll('tr')
        for det in product_details:
            try:
                key_name = det.find('span', {'class': 'a-text-bold'}).text
            except:
                key_name = det.find('th', {'class': 'prodDetSectionEntry'}).text
            main_details[key_name.split('\n')[0].strip()] = det.text.replace(key_name, '').replace('\u200e', '').strip()

        try:
            additional_info = product.find('div', {'id': 'detailBullets_feature_div'}).findAll('ul', {
                'class': 'detail-bullet-list'})
            for let in additional_info:
                try:
                    key_name = let.find('span', {'class': 'a-text-bold'}).text
                except:
                    key_name = let.find().text
                if key_name not in main_details:
                    main_details[key_name.split('\n')[0].strip()] = let.text.replace(key_name, '').replace('\u200e',
                                                                                                           '').strip()
        except AttributeError:
            pass

        try:
            main_details['Customer reviews'] = {
                star.findAll('td')[0].text.strip(): star.findAll('td')[-1].text.strip().replace('%', '') for star in
                product.findAll('tr', {'class': 'a-histogram-row a-align-center'})}
        except AttributeError:
            pass

        try:
            main_details['Reviews'] = []
            customer_reviews = product.find('div', {'id': 'cm-cr-dp-review-list'}).findAll('div',
                                                                                           {'data-hook': 'review'})
            for rev in customer_reviews:
                temp_rev = {}
                temp_rev['user_name'] = rev.find('div', {'class': 'a-profile-content'}).text.strip()
                temp_rev['rating'] = rev.find('i', {'data-hook': 'review-star-rating'}).find('span').text.strip()
                temp_rev['title'] = rev.find('i', {'data-hook': 'review-star-rating'}).findAll('span')[-1].text.strip()
                temp_rev['date'] = rev.find('span', {'data-hook': 'review-date'}).text.strip()
                temp_rev['size'] = \
                    rev.find('span', {'data-hook': 'format-strip-linkless'}).text.strip().split('Size: ')[-1].split(
                        'Color: ')[
                        0]
                temp_rev['color'] = \
                    rev.find('span', {'data-hook': 'format-strip-linkless'}).text.strip().split('Color: ')[
                        -1]
                temp_rev['body'] = rev.find('span', {'data-hook': 'review-collapsed'}).text.strip()
                main_details['Reviews'].append(temp_rev)
        except AttributeError as exc:
            if "'NoneType' object has no attribute 'findAll'" in str(exc):
                pass
            else:
                raise exc
        return main_details
